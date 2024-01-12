package go_mysql

import (
	sql2 "database/sql"
	"fmt"
	go_format "github.com/pefish/go-format"
	go_time "github.com/pefish/go-time"
	"github.com/pkg/errors"
	"reflect"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pefish/go-logger"
	"github.com/pefish/go-mysql/sqlx"
	uuid "github.com/satori/go.uuid"
)

type IMysql interface {
	TagName() string
	SetLogger(logger go_logger.InterfaceLogger)
	Close()

	ConnectWithConfiguration(configuration Configuration) error

	RawSelectByStr(
		dest interface{},
		select_ string,
		str string,
		values ...interface{},
	) error
	RawExec(sql string, values ...interface{}) (uint64, uint64, error)
	Count(countParams *CountParams, values ...interface{}) (uint64, error)
	RawCount(sql string, values ...interface{}) (uint64, error)
	Sum(sumParams *SumParams, values ...interface{}) (uint64, error)
	SelectFirst(dest interface{}, selectParams *SelectParams, values ...interface{}) (bool, error)
	SelectById(
		dest interface{},
		selectByIdParams *SelectByIdParams,
	) (notFound bool, err error)
	Select(dest interface{}, selectParams *SelectParams, values ...interface{}) error
	Insert(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error)
	Update(
		updateParams *UpdateParams,
		values ...interface{},
	) (lastInsertId uint64, rowsAffected uint64, err error)

	Begin() (*MysqlClass, error)
	Commit() error
	Rollback() error
}

type Configuration struct {
	Host            string
	Port            int
	Username        string
	Password        string
	Database        string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnParams      map[string]string
}

var (
	DEFAULT_PORT              int = 3306
	DEFAULT_MAX_OPEN_CONNS    int = 100
	DEFAULT_MAX_IDLE_CONNS    int = 30
	DEFAULT_CONN_MAX_LIFTTIME     = 6 * time.Second
)

var MysqlInstance IMysql = NewMysqlInstance()

// ----------------------------- MysqlClass -----------------------------

type MysqlClass struct {
	Db      *sqlx.DB
	TxId    string
	Tx      *sqlx.Tx
	tagName string
	logger  go_logger.InterfaceLogger
}

func NewMysqlInstance() *MysqlClass {
	return &MysqlClass{
		tagName: `json`,
		logger:  go_logger.Logger,
	}
}

func (mc *MysqlClass) TagName() string {
	return mc.tagName
}

func (mc *MysqlClass) SetLogger(logger go_logger.InterfaceLogger) {
	mc.logger = logger
}

func (mc *MysqlClass) Close() {
	if mc.Db != nil {
		err := mc.Db.Close()
		if err != nil {
			mc.logger.Error(err)
		} else {
			mc.logger.Info(`mysql close succeed.`)
		}
	}
	if mc.Tx != nil {
		err := mc.Tx.Rollback()
		if err != nil {
			mc.logger.Error(err)
		}
	}
}

func (mc *MysqlClass) ConnectWithConfiguration(configuration Configuration) error {
	var port = DEFAULT_PORT
	if configuration.Port != 0 {
		port = configuration.Port
	}
	var database string
	if configuration.Database != "" {
		database = configuration.Database
	}
	var maxOpenConns = DEFAULT_MAX_OPEN_CONNS
	if configuration.MaxOpenConns != 0 {
		maxOpenConns = configuration.MaxOpenConns
	}
	var maxIdleConns = DEFAULT_MAX_IDLE_CONNS
	if configuration.MaxIdleConns != 0 {
		maxIdleConns = configuration.MaxIdleConns
	}
	connMaxLifetime := DEFAULT_CONN_MAX_LIFTTIME
	if configuration.ConnMaxLifetime != 0 {
		connMaxLifetime = configuration.ConnMaxLifetime
	}

	address := fmt.Sprintf(`%s:%d`, configuration.Host, port)
	mc.logger.Info(fmt.Sprintf(`mysql connecting... url: %s`, address))

	connParamsStr := "charset=utf8&parseTime=true&multiStatements=true&loc=UTC"
	if configuration.ConnParams != nil {
		for k, v := range configuration.ConnParams {
			connParamsStr += fmt.Sprintf("&%s=%s", k, v)
		}
	}
	connUrl := fmt.Sprintf(
		`%s:%s@tcp(%s)/%s?%s`,
		configuration.Username,
		configuration.Password,
		address,
		database,
		connParamsStr,
	)
	db, err := sqlx.Connect(`mysql`, connUrl)
	if err != nil {
		return err
	}
	db.SetTagName(mc.tagName)
	mc.logger.Info(fmt.Sprintf(`mysql connect succeed. url: %s`, address))
	db.DB.SetMaxOpenConns(maxOpenConns)       // 用于设置最大打开的连接数，默认值为0表示不限制
	db.DB.SetMaxIdleConns(maxIdleConns)       // 用于设置闲置的连接数
	db.DB.SetConnMaxLifetime(connMaxLifetime) // 设置一个超时时间，时间小于数据库的超时时间即可
	mc.Db = db

	return nil
}

func (mc *MysqlClass) printDebugInfo(sql string, values interface{}) {
	txInfo := ``
	if mc.Tx != nil {
		txInfo = fmt.Sprintf(`[transaction id: %s] `, mc.TxId)
	}
	mc.logger.DebugF(`%s%s, %v`, txInfo, sql, values)
}

func (mc *MysqlClass) RawSelectByStr(
	dest interface{},
	select_ string,
	str string,
	values ...interface{},
) error {
	select_ = mc.replaceIfStar(dest, select_)
	sql := fmt.Sprintf(
		`select %s %s`,
		select_,
		str,
	)
	err := mc.rawSelect(dest, sql, values...)
	if err != nil {
		return err
	}
	return nil
}

func (mc *MysqlClass) processValues(sql string, values []interface{}) (string, []interface{}, error) {
	hasArr := false
	for _, v := range values {
		rt := reflect.TypeOf(v)
		if rt.Kind() == reflect.Array || rt.Kind() == reflect.Slice {
			hasArr = true
			break
		}
	}
	if hasArr {
		var err error
		sql, values, err = sqlx.In(sql, values...)
		if err != nil {
			return ``, nil, errors.WithStack(err)
		}
	}
	return sql, values, nil
}

func (mc *MysqlClass) MustRawExec(sql string, values ...interface{}) (uint64, uint64) {
	lastInsertId, rowsAffected, err := mc.RawExec(sql, values...)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (mc *MysqlClass) RawExec(sql string, values ...interface{}) (uint64, uint64, error) {
	sql, values, err := mc.processValues(sql, values)
	mc.printDebugInfo(sql, values)
	if err != nil {
		return 0, 0, err
	}

	var result sql2.Result
	if mc.Tx != nil {
		result, err = mc.Tx.Exec(sql, values...)
	} else {
		result, err = mc.Db.Exec(sql, values...)
	}
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	lastInsertId, err := result.LastInsertId()
	if err != nil {
		return 0, 0, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	return uint64(lastInsertId), uint64(rowsAffected), nil
}

func (mc *MysqlClass) correctSelectStar(dest interface{}, sql string) string {
	sql = strings.TrimLeft(sql, " \n\t")
	if strings.HasPrefix(sql, `select *`) {
		return `select ` + mc.replaceIfStar(dest, "*") + sql[8:]
	}

	return sql
}

func (mc *MysqlClass) replaceIfStar(dest interface{}, str string) string {
	if str == "*" {
		tags := go_format.FormatInstance.GetValuesInTagFromStruct(dest, mc.tagName)
		if len(tags) == 0 {
			return str
		}
		return strings.Join(tags, `,`)
	}

	return str
}

func (mc *MysqlClass) rawSelect(
	dest interface{},
	sql string,
	values ...interface{},
) error {
	sql = mc.correctSelectStar(dest, sql)
	sql, values, err := mc.processValues(sql, values)
	mc.printDebugInfo(sql, values)
	if err != nil {
		return err
	}
	if mc.Tx != nil {
		err = mc.Tx.Select(dest, sql, values...)
	} else {
		err = mc.Db.Select(dest, sql, values...)
	}
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

type CountParams struct {
	TableName string
	Where     interface{}
}

func (mc *MysqlClass) Count(countParams *CountParams, values ...interface{}) (uint64, error) {
	var countStruct struct {
		Count uint64 `json:"count"`
	}

	paramArgs, whereStr, err := builder.buildWhere(countParams.Where, values)
	if err != nil {
		return 0, err
	}

	sql := fmt.Sprintf(
		`select count(*) as count from %s %s`,
		countParams.TableName,
		whereStr,
	)

	_, err = mc.rawSelectFirst(&countStruct, sql, paramArgs...)
	if err != nil {
		return 0, err
	}
	return countStruct.Count, nil
}

func (mc *MysqlClass) RawCount(sql string, values ...interface{}) (uint64, error) {
	var countStruct struct {
		Count uint64 `json:"count"`
	}
	sql, values, err := mc.processValues(sql, values)
	mc.printDebugInfo(sql, values)
	if err != nil {
		return 0, err
	}
	if mc.Tx != nil {
		err = mc.Tx.Select(&countStruct, sql, values...)
	} else {
		err = mc.Db.Select(&countStruct, sql, values...)
	}
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return countStruct.Count, nil
}

type SumParams struct {
	TableName string
	SumTarget string
	Where     interface{}
}

func (mc *MysqlClass) Sum(
	sumParams *SumParams,
	values ...interface{},
) (uint64, error) {
	var sumStruct struct {
		Sum *string `json:"sum"`
	}

	paramArgs, whereStr, err := builder.buildWhere(sumParams.Where, values)
	if err != nil {
		return 0, err
	}

	sql := fmt.Sprintf(
		`select sum("%s") as sum from %s %s`,
		sumParams.SumTarget,
		sumParams.TableName,
		whereStr,
	)

	_, err = mc.rawSelectFirst(&sumStruct, sql, paramArgs...)
	if err != nil {
		return 0, err
	}
	if sumStruct.Sum == nil {
		return 0, nil
	}
	return go_format.FormatInstance.MustToUint64(*sumStruct.Sum), nil
}

type SelectParams struct {
	TableName string
	Select    string
	Where     interface{}
	OrderBy   string
	Limit     string
}

func (mc *MysqlClass) SelectFirst(
	dest interface{},
	selectParams *SelectParams,
	values ...interface{},
) (bool, error) {
	selectParams.Select = mc.replaceIfStar(dest, selectParams.Select)
	sql, paramArgs, err := builder.buildSelectSql(selectParams, values...)
	if err != nil {
		return true, err
	}
	return mc.rawSelectFirst(dest, sql, paramArgs...)
}

type SelectByIdParams struct {
	TableName string
	Select    string
	Id        uint64
}

func (mc *MysqlClass) SelectById(
	dest interface{},
	selectByIdParams *SelectByIdParams,
) (notFound bool, err error) {
	select_ := mc.replaceIfStar(dest, selectByIdParams.Select)
	paramArgs := make([]interface{}, 0)
	sql, paramArgs, err := builder.buildSelectSql(
		&SelectParams{
			TableName: selectByIdParams.TableName,
			Select:    select_,
			Where: map[string]interface{}{
				`id`: selectByIdParams.Id,
			},
		},
	)
	if err != nil {
		return true, err
	}
	return mc.rawSelectFirst(dest, sql, paramArgs...)
}

func (mc *MysqlClass) Select(
	dest interface{},
	selectParams *SelectParams,
	values ...interface{},
) error {
	selectParams.Select = mc.replaceIfStar(dest, selectParams.Select)
	sql, paramArgs, err := builder.buildSelectSql(selectParams, values...)
	if err != nil {
		return err
	}
	err = mc.rawSelect(dest, sql, paramArgs...)
	if err != nil {
		return err
	}
	return nil
}

func (mc *MysqlClass) Insert(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := builder.buildInsertSql(tableName, params)
	if err != nil {
		return 0, 0, err
	}
	return mc.RawExec(sql, paramArgs...)
}

func (mc *MysqlClass) InsertIgnore(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := builder.buildInsertSql(tableName, params)
	if err != nil {
		return 0, 0, err
	}
	return mc.RawExec(sql, paramArgs...)
}

type UpdateParams struct {
	TableName string
	Update    interface{}
	Where     interface{}
}

func (mc *MysqlClass) Update(updateParams *UpdateParams, values ...interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := builder.buildUpdateSql(updateParams, values...)
	if err != nil {
		return 0, 0, err
	}
	return mc.RawExec(sql, paramArgs...)
}

func (mc *MysqlClass) rawSelectFirst(dest interface{}, sql string, values ...interface{}) (bool, error) {
	sql = mc.correctSelectStar(dest, sql)
	sql, values, err := mc.processValues(sql, values)
	mc.printDebugInfo(sql, values)
	if err != nil {
		return true, err
	}

	if mc.Tx != nil {
		err = mc.Tx.Get(dest, sql, values...)
	} else {
		err = mc.Db.Get(dest, sql, values...)
	}
	if err != nil {
		if err.Error() == `sql: no rows in result set` {
			return true, nil
		} else {
			return true, errors.WithStack(err)
		}
	}

	return false, nil
}

func (mc *MysqlClass) Begin() (*MysqlClass, error) {
	id := fmt.Sprintf(`%s`, uuid.NewV4())
	mc.printDebugInfo(`begin`, nil)
	tx, err := mc.Db.Beginx()
	if err != nil {
		return nil, err
	}
	return &MysqlClass{
		Db:      nil,
		TxId:    id,
		Tx:      tx,
		tagName: mc.tagName,
		logger:  mc.logger,
	}, nil
}

func (mc *MysqlClass) Commit() error {
	mc.printDebugInfo(`commit`, nil)

	err := mc.Tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (mc *MysqlClass) Rollback() error {
	mc.printDebugInfo(`rollback`, nil)

	err := mc.Tx.Rollback()
	if err != nil {
		return err
	}
	return nil
}

// ----------------------------- builderClass -----------------------------

type builderClass struct {
}

var builder = builderClass{}

func (mysql *builderClass) buildInsertSql(tableName string, params interface{}) (string, []interface{}, error) {
	var cols []string
	var vals []string
	var paramArgs = make([]interface{}, 0)
	type_ := reflect.TypeOf(params)
	switch type_.Kind() {
	case reflect.Map:
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			cols, _, vals, paramArgs = mysql.buildFromMap(params.(map[string]interface{}))
		} else {
			return ``, nil, errors.New(`Map value type error.`)
		}
	case reflect.Struct:
		map_ := make(map[string]interface{})
		err := mysql.structToMap(params, map_)
		if err != nil {
			return ``, nil, err
		}
		cols, _, vals, paramArgs = mysql.buildFromMap(map_)
	case reflect.Slice:
		value_ := reflect.ValueOf(params)
		if value_.Len() == 0 {
			return "", nil, errors.New("Slice length cannot be 0.")
		}
		map_ := make(map[string]interface{})
		err := mysql.structToMap(value_.Index(0).Interface(), map_)
		if err != nil {
			return ``, nil, err
		}
		for key, _ := range map_ {
			cols = append(cols, key)
		}
		q := squirrel.Insert(tableName).Columns(cols...)
		for i := 0; i < value_.Len(); i++ {
			map_ := make(map[string]interface{})
			err := mysql.structToMap(value_.Index(i).Interface(), map_)
			if err != nil {
				return ``, nil, err
			}
			vals := make([]interface{}, 0, 5)
			for _, colName := range cols {
				vals = append(vals, map_[colName])
			}
			q = q.Values(vals...)
		}
		return q.ToSql()
	default:
		return ``, nil, errors.New(`Type error.`)
	}

	insertStr := `insert`
	str := fmt.Sprintf(
		`%s into %s (%s) values (%s)`,
		insertStr,
		tableName,
		strings.Join(cols, `,`),
		strings.Join(vals, `,`),
	)
	return str, paramArgs, nil
}

func (mysql *builderClass) buildWhereFromMap(ele map[string]interface{}) ([]interface{}, string) {
	cols, ops, vals, args := mysql.buildFromMap(ele)

	andStr := ``
	for i, col := range cols {
		andStr = andStr + col + " " + ops[i] + " " + vals[i] + ` and `
	}
	if len(andStr) > 4 {
		andStr = andStr[:len(andStr)-5]
	}
	return args, andStr
}

func (mysql *builderClass) buildFromMap(ele map[string]interface{}) (cols []string, ops, vals []string, args []interface{}) {
	cols = make([]string, 0)
	ops = make([]string, 0)
	vals = make([]string, 0)
	args = make([]interface{}, 0)
	for key, val := range ele {
		if val == nil {
			continue
		}

		kind := reflect.TypeOf(val).Kind()
		if kind == reflect.Slice {
			value_ := reflect.ValueOf(val)
			if value_.Len() == 0 {
				continue
			}
			args_ := make([]interface{}, 0)
			vals_ := make([]string, 0)
			for i := 0; i < value_.Len(); i++ {
				str := go_format.FormatInstance.ToString(value_.Index(i).Interface())
				if str == "" {
					continue
				}
				vals_ = append(vals_, "?")
				args_ = append(args_, str)
			}
			if len(vals_) == 0 {
				continue
			}

			cols = append(cols, key)
			ops = append(ops, "in")
			vals = append(vals, fmt.Sprintf("(%s)", strings.Join(vals_, ",")))
			args = append(args, args_...)
		} else {
			cols = append(cols, key)
			str := go_format.FormatInstance.ToString(val)
			if strings.HasPrefix(str, `s:`) {
				r := strings.Trim(str[2:], " ")
				index := strings.Index(r, " ")
				ops = append(ops, r[:index])
				vals = append(vals, r[index+1:])
			} else {
				ops = append(ops, "=")
				vals = append(vals, "?")
				args = append(args, str)
			}
		}
	}
	return
}

func (mysql *builderClass) buildWhere(where interface{}, args []interface{}) (paramArgs []interface{}, whereSql string, err error) {
	if where == nil {
		return make([]interface{}, 0), "", nil
	}
	type_ := reflect.TypeOf(where)
	paramArgs = args
	str := ``
	switch type_.Kind() {
	case reflect.Map:
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			paramArgs, str = mysql.buildWhereFromMap(where.(map[string]interface{}))
		} else {
			return nil, ``, errors.New(`map value type error`)
		}
	case reflect.Struct:
		map_ := make(map[string]interface{})
		err := mysql.structToMap(where, map_)
		if err != nil {
			return nil, ``, err
		}
		paramArgs, str = mysql.buildWhereFromMap(map_)
	case reflect.Slice:
		if type_.Elem().Kind() != reflect.Map {
			return nil, ``, errors.New(`slice value type error`)
		}
		mapKind := type_.Elem().Elem().Kind()
		if mapKind == reflect.Interface {
			sliceVal := where.([]map[string]interface{})
			for _, ele := range sliceVal {
				paramArgsTemp, str_ := mysql.buildWhereFromMap(ele)
				paramArgs = append(paramArgs, paramArgsTemp...)
				str += `(` + str_ + `) or `
			}
		} else {
			return nil, ``, errors.New(`map value type error`)
		}
		if len(str) > 3 {
			str = str[:len(str)-4]
		}
	case reflect.String:
		return paramArgs, "where " + where.(string), nil
	default:
		return nil, ``, errors.New(`Where type error.`)
	}

	if str != "" {
		return paramArgs, "where " + str, nil
	}
	return paramArgs, "", nil
}

func (mysql *builderClass) buildSelectSql(selectParams *SelectParams, values ...interface{}) (sql string, paramArgs []interface{}, err error) {
	paramArgs, whereStr, err := mysql.buildWhere(selectParams.Where, values)
	if err != nil {
		return ``, nil, err
	}

	str := fmt.Sprintf(
		`select %s from %s %s`,
		selectParams.Select,
		selectParams.TableName,
		whereStr,
	)
	if selectParams.OrderBy != "" {
		str += fmt.Sprintf(" order by %s", selectParams.OrderBy)
	}
	if selectParams.Limit != "" {
		str += fmt.Sprintf(" limit %s", selectParams.Limit)
	}
	return str, paramArgs, nil
}

func (mysql *builderClass) structToMap(in_ interface{}, result map[string]interface{}) error {
	objVal := reflect.ValueOf(in_)
	if objVal.Kind() == reflect.Ptr {
		objVal = objVal.Elem()
	}
	if objVal.Kind() == reflect.Map {
		for k, v := range in_.(map[string]interface{}) {
			result[k] = v
		}
		return nil
	}

	if objVal.Kind() != reflect.Struct {
		return errors.New("Must be struct type.")
	}
	objType := objVal.Type()
	for i := 0; i < objVal.NumField(); i++ {
		field := objVal.Field(i)
		fieldType := objType.Field(i)

		key := fieldType.Name
		jsonTag := fieldType.Tag.Get("json")
		if jsonTag != "" {
			jsonTags := strings.Split(jsonTag, ",")
			if len(jsonTags) > 1 && jsonTags[1] == "omitempty" && go_format.FormatInstance.IsZeroValue(field) { // 如果标记了omitempty且是零值，则不映射到 map 中
				continue
			}
			key = jsonTags[0]
		}

		strValue := ""
		if fieldType.Type.String() == "time.Time" {
			strValue = go_time.TimeInstance.TimeToStr(field.Interface().(time.Time), "0000-00-00 00:00:00")
		} else if field.Kind() == reflect.Struct {
			err := mysql.structToMap(field.Interface(), result)
			if err != nil {
				return err
			}
			continue
		} else {
			strValue = go_format.FormatInstance.ToString(field.Interface())
		}
		result[key] = strValue
	}
	return nil
}

func (mysql *builderClass) buildUpdateSql(updateParams *UpdateParams, values ...interface{}) (string, []interface{}, error) {
	var updateStr = ``
	paramArgs := make([]interface{}, 0)
	type_ := reflect.TypeOf(updateParams.Update)
	switch type_.Kind() {
	case reflect.Map:
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			for key, val := range updateParams.Update.(map[string]interface{}) {
				if val == nil {
					continue
				}
				updateStr = updateStr + key + ` = ?,`
				paramArgs = append(paramArgs, go_format.FormatInstance.ToString(val))
			}
		} else {
			return ``, nil, errors.New(`map value type error`)
		}
	case reflect.Struct:
		map_ := make(map[string]interface{})
		err := mysql.structToMap(updateParams.Update, map_)
		if err != nil {
			return ``, nil, err
		}
		for key, val := range map_ {
			if val == nil {
				continue
			}
			updateStr = updateStr + key + ` = ?,`
			paramArgs = append(paramArgs, go_format.FormatInstance.ToString(val))
		}
	default:
		return ``, nil, errors.New(`Type error.`)

	}

	if len(updateStr) > 0 {
		updateStr = updateStr[:len(updateStr)-1]
	}

	paramArgsTemp, whereStr, err := mysql.buildWhere(updateParams.Where, values)
	if err != nil {
		return ``, nil, err
	}
	paramArgs = append(paramArgs, paramArgsTemp...)

	str := fmt.Sprintf(
		`update %s set %s %s`,
		updateParams.TableName,
		updateStr,
		whereStr,
	)
	return str, paramArgs, nil
}
