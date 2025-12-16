package go_mysql

import (
	sql2 "database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	go_format "github.com/pefish/go-format"
	go_format_any "github.com/pefish/go-format/any"
	go_time "github.com/pefish/go-time"
	"github.com/pkg/errors"

	_ "github.com/go-sql-driver/mysql"
	i_logger "github.com/pefish/go-interface/i-logger"
	i_mysql "github.com/pefish/go-interface/i-mysql"
	t_mysql "github.com/pefish/go-interface/t-mysql"
	"github.com/pefish/go-mysql/sqlx"
)

var (
	DEFAULT_PORT              int = 3306
	DEFAULT_MAX_OPEN_CONNS    int = 100
	DEFAULT_MAX_IDLE_CONNS    int = 30
	DEFAULT_CONN_MAX_LIFTTIME     = 6 * time.Second
)

var ErrorNoAffectedRows error = errors.New("No affected rows.")

// ----------------------------- MysqlClass -----------------------------

type MysqlType struct {
	db      *sqlx.DB
	txId    string
	tx      *sqlx.Tx
	tagName string
	logger  i_logger.ILogger
}

func NewMysqlInstance(logger i_logger.ILogger) *MysqlType {
	return &MysqlType{
		tagName: `json`,
		logger:  logger,
	}
}

func (mc *MysqlType) TagName() string {
	return mc.tagName
}

func (mc *MysqlType) Close() {
	if mc.db != nil {
		err := mc.db.Close()
		if err != nil {
			mc.logger.Error(err)
		} else {
			mc.logger.Info(`mysql close succeed.`)
		}
	}
	if mc.tx != nil {
		err := mc.tx.Rollback()
		if err != nil {
			mc.logger.Error(err)
		}
	}
}

func (mc *MysqlType) ConnectWithConfiguration(configuration t_mysql.Configuration) error {
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

	connParamsStr := "parseTime=true&multiStatements=true&loc=UTC"
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
	mc.db = db

	return nil
}

func (mc *MysqlType) printDebugInfo(sql string, values any) {
	txInfo := ``
	if mc.tx != nil {
		txInfo = fmt.Sprintf(`[transaction id: %s] `, mc.txId)
	}
	mc.logger.DebugF("%s%s, %v\n", txInfo, sql, values)
}

func (mc *MysqlType) RawSelect(
	dest any,
	select_ string,
	str string,
	values ...any,
) error {
	sql := str
	if select_ != "" {
		select_ = mc.replaceIfStar(dest, select_)
		sql = fmt.Sprintf(
			`select %s %s`,
			select_,
			str,
		)
	}

	err := mc.rawSelect(dest, sql, values...)
	if err != nil {
		return err
	}
	return nil
}

func (mc *MysqlType) RawSelectFirst(
	dest any,
	select_ string,
	str string,
	values ...any,
) (
	notFound bool,
	err error,
) {
	sql := str
	if select_ != "" {
		select_ = mc.replaceIfStar(dest, select_)
		sql = fmt.Sprintf(
			`select %s %s`,
			select_,
			str,
		)
	}
	return mc.rawSelectFirst(dest, sql, values...)
}

func (mc *MysqlType) processValues(sql string, values []any) (string, []any, error) {
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

func (mc *MysqlType) MustRawExec(sql string, values ...any) uint64 {
	lastInsertId, err := mc.RawExec(sql, values...)
	if err != nil {
		panic(err)
	}
	return lastInsertId
}

func (mc *MysqlType) RawExec(sql string, values ...any) (
	lastInsertId uint64,
	err error,
) {
	sql, values, err = mc.processValues(sql, values)
	mc.printDebugInfo(sql, values)
	if err != nil {
		return 0, err
	}

	var result sql2.Result
	if mc.tx != nil {
		result, err = mc.tx.Exec(sql, values...)
	} else {
		result, err = mc.db.Exec(sql, values...)
	}
	if err != nil {
		return 0, errors.WithStack(err)
	}
	lastInsertId_, err := result.LastInsertId()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if rowsAffected == 0 {
		return 0, ErrorNoAffectedRows
	}
	return uint64(lastInsertId_), nil
}

func (mc *MysqlType) replaceIfStar(dest any, str string) string {
	if str == "*" {
		tags := go_format.FetchTags(dest, mc.tagName)
		if len(tags) == 0 {
			return str
		}
		return "`" + strings.Join(tags, "`,`") + "`"
	}

	return str
}

func (mc *MysqlType) rawSelect(
	dest any,
	sql string,
	values ...any,
) error {
	sql, values, err := mc.processValues(sql, values)
	mc.printDebugInfo(sql, values)
	if err != nil {
		return err
	}
	if mc.tx != nil {
		err = mc.tx.Select(dest, sql, values...)
	} else {
		err = mc.db.Select(dest, sql, values...)
	}
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (mc *MysqlType) Count(countParams *t_mysql.CountParams, values ...any) (
	count uint64,
	err error,
) {
	var countStruct struct {
		Count uint64 `json:"count"`
	}

	paramArgs, whereStr, err := builder.buildWhere(countParams.Where, values)
	if err != nil {
		return 0, err
	}

	sql := fmt.Sprintf(
		"select count(*) as count from `%s` %s",
		countParams.TableName,
		whereStr,
	)

	_, err = mc.rawSelectFirst(&countStruct, sql, paramArgs...)
	if err != nil {
		return 0, err
	}
	return countStruct.Count, nil
}

func (mc *MysqlType) RawCount(sql string, values ...any) (
	count uint64,
	err error,
) {
	var countStruct struct {
		Count uint64 `json:"count"`
	}
	sql, values, err = mc.processValues(sql, values)
	mc.printDebugInfo(sql, values)
	if err != nil {
		return 0, err
	}
	if mc.tx != nil {
		err = mc.tx.Select(&countStruct, sql, values...)
	} else {
		err = mc.db.Select(&countStruct, sql, values...)
	}
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return countStruct.Count, nil
}

func (mc *MysqlType) Sum(
	sumParams *t_mysql.SumParams,
	values ...any,
) (
	sum float64,
	err error,
) {
	var sumStruct struct {
		Sum *string `json:"sum"`
	}

	paramArgs, whereStr, err := builder.buildWhere(sumParams.Where, values)
	if err != nil {
		return 0, err
	}

	sql := fmt.Sprintf(
		"select sum(`%s`) as sum from `%s` %s",
		sumParams.SumTarget,
		sumParams.TableName,
		whereStr,
	)

	_, err = mc.rawSelectFirst(&sumStruct, sql, paramArgs...)
	if err != nil {
		return 0, err
	}

	if sumStruct.Sum == nil || *sumStruct.Sum == "" {
		return 0, nil
	}
	return go_format_any.MustToFloat64(*sumStruct.Sum), nil
}

func (mc *MysqlType) SelectFirst(
	dest any,
	selectParams *t_mysql.SelectParams,
	values ...any,
) (
	notFound bool,
	err error,
) {
	selectParams.Select = mc.replaceIfStar(dest, selectParams.Select)
	sql, paramArgs, err := builder.buildSelectSql(selectParams, values...)
	if err != nil {
		return true, err
	}
	return mc.rawSelectFirst(dest, sql, paramArgs...)
}

func (mc *MysqlType) SelectById(
	dest any,
	selectByIdParams *t_mysql.SelectByIdParams,
) (
	notFound bool,
	err error,
) {
	select_ := mc.replaceIfStar(dest, selectByIdParams.Select)
	sql, paramArgs, err := builder.buildSelectSql(
		&t_mysql.SelectParams{
			TableName: selectByIdParams.TableName,
			Select:    select_,
			Where: map[string]any{
				`id`: selectByIdParams.Id,
			},
		},
	)
	if err != nil {
		return true, err
	}
	return mc.rawSelectFirst(dest, sql, paramArgs...)
}

func (mc *MysqlType) Select(
	dest any,
	selectParams *t_mysql.SelectParams,
	values ...any,
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

func (mc *MysqlType) Insert(tableName string, params any) (
	lastInsertId uint64,
	err error,
) {
	sql, paramArgs, err := builder.buildInsertSql(tableName, params, false)
	if err != nil {
		return 0, err
	}
	return mc.RawExec(sql, paramArgs...)
}

func (mc *MysqlType) InsertIgnore(tableName string, params any) (
	lastInsertId uint64,
	err error,
) {
	sql, paramArgs, err := builder.buildInsertSql(tableName, params, true)
	if err != nil {
		return 0, err
	}
	return mc.RawExec(sql, paramArgs...)
}

func (mc *MysqlType) Update(updateParams *t_mysql.UpdateParams, values ...any) (
	lastInsertId uint64,
	err error,
) {
	sql, paramArgs, err := builder.buildUpdateSql(updateParams, values...)
	if err != nil {
		return 0, err
	}
	return mc.RawExec(sql, paramArgs...)
}

func (mc *MysqlType) rawSelectFirst(dest any, sql string, values ...any) (
	notFound bool,
	err error,
) {
	sql, values, err = mc.processValues(sql, values)
	mc.printDebugInfo(sql, values)
	if err != nil {
		return true, err
	}

	if mc.tx != nil {
		err = mc.tx.Get(dest, sql, values...)
	} else {
		err = mc.db.Get(dest, sql, values...)
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

func (mc *MysqlType) Begin() (i_mysql.IMysql, error) {
	id := uuid.New().String()
	mc.printDebugInfo(`begin`, nil)
	tx, err := mc.db.Beginx()
	if err != nil {
		return nil, err
	}
	return &MysqlType{
		db:      nil,
		txId:    id,
		tx:      tx,
		tagName: mc.tagName,
		logger:  mc.logger,
	}, nil
}

func (mc *MysqlType) Commit() error {
	mc.printDebugInfo(`commit`, nil)

	err := mc.tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (mc *MysqlType) Rollback() error {
	mc.printDebugInfo(`rollback`, nil)

	err := mc.tx.Rollback()
	if err != nil {
		return err
	}
	return nil
}

// ----------------------------- builderClass -----------------------------

type builderClass struct {
}

var builder = builderClass{}

func (mysql *builderClass) buildInsertSql(
	tableName string,
	params any,
	ignoreIfDuplicate bool,
) (
	sql string,
	paramArgs []any,
	err error,
) {
	cols := make([][]string, 0)
	vals := make([]string, 0) // ["(?,?)","(?,?)"]
	type_ := reflect.TypeOf(params)
	var buildStructOrMap = func(params any) (
		cols []string,
		mapVals []string,
		paramArgs []any,
		err error,
	) {
		map_ := make(map[string]any)
		err = mysql.structToMap(params, map_)
		if err != nil {
			return nil, nil, nil, err
		}
		for key, val := range map_ {
			cols = append(cols, key)
			mapVals = append(mapVals, "?")
			paramArgs = append(paramArgs, go_format_any.ToString(val))
		}
		return
	}
	switch type_.Kind() {
	case reflect.Struct, reflect.Map, reflect.Pointer:
		cols_, mapVals, paramArgs_, err := buildStructOrMap(params)
		if err != nil {
			return ``, nil, err
		}
		cols = append(cols, cols_)
		paramArgs = append(paramArgs, paramArgs_...)
		vals = append(vals, fmt.Sprintf("(%s)", strings.Join(mapVals, ",")))
	case reflect.Slice:
		// INSERT INTO table (a,b) VALUES (?,?),(?,?)
		value_ := reflect.ValueOf(params)
		if value_.Len() == 0 {
			return "", nil, errors.New("Slice length cannot be 0.")
		}
		for i := 0; i < value_.Len(); i++ {
			cols_, mapVals, paramArgs_, err := buildStructOrMap(value_.Index(i).Interface())
			if err != nil {
				return ``, nil, err
			}
			newParamArgs_ := make([]any, len(paramArgs_))
			copy(newParamArgs_, paramArgs_)
			if len(cols) > 0 {
				if len(cols_) != len(cols[len(cols)-1]) {
					return ``, nil, errors.New("Slice length not match.")
				}
				// 对齐顺序
				for i_, col := range cols[0] {
					for j_, col_ := range cols_ {
						if col == col_ {
							newParamArgs_[i_] = paramArgs_[j_]
						}
					}
				}
			}
			cols = append(cols, cols_)
			paramArgs = append(paramArgs, newParamArgs_...)
			vals = append(vals, fmt.Sprintf("(%s)", strings.Join(mapVals, ",")))
		}
	default:
		return ``, nil, errors.New(`Type error.`)
	}

	str := fmt.Sprintf(
		"insert%s into `%s` (`%s`) values %s",
		func() string {
			if ignoreIfDuplicate {
				return " ignore"
			} else {
				return ""
			}
		}(),
		tableName,
		strings.Join(cols[0], "`,`"),
		strings.Join(vals, `,`),
	)
	return str, paramArgs, nil
}

func (mysql *builderClass) buildWhereFromMap(ele map[string]any) (
	args []any,
	andStr string,
) {
	cols, ops, vals, args := mysql.buildFromMap(ele)

	for i, col := range cols {
		andStr = andStr + fmt.Sprintf("`%s` %s %s and ", col, ops[i], vals[i])
	}
	if len(andStr) > 4 {
		andStr = andStr[:len(andStr)-5]
	}
	return args, andStr
}

func (mysql *builderClass) buildFromMap(ele map[string]any) (
	cols []string,
	ops,
	vals []string,
	args []any,
) {
	cols = make([]string, 0)
	ops = make([]string, 0)
	vals = make([]string, 0)
	args = make([]any, 0)
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
			args_ := make([]any, 0)
			vals_ := make([]string, 0)
			for i := 0; i < value_.Len(); i++ {
				str := go_format_any.ToString(value_.Index(i).Interface())
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
			str := go_format_any.ToString(val)
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

func (mysql *builderClass) buildWhere(where any, args []any) (
	paramArgs []any,
	whereSql string,
	err error,
) {
	if where == nil {
		return make([]any, 0), "", nil
	}
	type_ := reflect.TypeOf(where)
	paramArgs = args
	str := ``
	switch type_.Kind() {
	case reflect.Map:
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			paramArgs, str = mysql.buildWhereFromMap(where.(map[string]any))
		} else {
			return nil, ``, errors.New(`map value type error`)
		}
	case reflect.Struct:
		map_ := make(map[string]any)
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
			sliceVal := where.([]map[string]any)
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

func (mysql *builderClass) buildSelectSql(selectParams *t_mysql.SelectParams, values ...any) (
	sql string,
	paramArgs []any,
	err error,
) {
	paramArgs, whereStr, err := mysql.buildWhere(selectParams.Where, values)
	if err != nil {
		return ``, nil, err
	}

	str := fmt.Sprintf(
		"select %s from `%s` %s",
		selectParams.Select,
		selectParams.TableName,
		whereStr,
	)
	if selectParams.OrderBy != nil {
		str += fmt.Sprintf(" order by `%s` %s", selectParams.OrderBy.Col, selectParams.OrderBy.Order)
	}
	if selectParams.Limit != 0 {
		str += fmt.Sprintf(" limit %d", selectParams.Limit)
	}
	return str, paramArgs, nil
}

func (mysql *builderClass) structToMap(in_ any, result map[string]any) error {
	objVal := reflect.ValueOf(in_)
	if objVal.Kind() == reflect.Ptr {
		objVal = objVal.Elem()
	}
	if objVal.Kind() == reflect.Map {
		for k, v := range in_.(map[string]any) {
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
			if len(jsonTags) > 1 && jsonTags[1] == "omitempty" && go_format.IsZeroValue(field) { // 如果标记了omitempty且是零值，则不映射到 map 中
				continue
			}
			key = jsonTags[0]
		}

		strValue := ""
		if fieldType.Type.String() == "time.Time" {
			strValue = go_time.TimeToStr(field.Interface().(time.Time), "0000-00-00 00:00:00")
		} else if field.Kind() == reflect.Struct {
			err := mysql.structToMap(field.Interface(), result)
			if err != nil {
				return err
			}
			continue
		} else {
			strValue = go_format_any.ToString(field.Interface())
		}
		result[key] = strValue
	}
	return nil
}

func (mysql *builderClass) buildUpdateSql(updateParams *t_mysql.UpdateParams, values ...any) (
	sql string,
	args []any,
	err error,
) {
	var updateStr = ``
	paramArgs := make([]any, 0)
	remainValues := values
	type_ := reflect.TypeOf(updateParams.Update)
	switch type_.Kind() {
	case reflect.Map:
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			for key, val := range updateParams.Update.(map[string]any) {
				if val == nil {
					continue
				}
				updateStr += fmt.Sprintf("`%s` = ?,", key)
				paramArgs = append(paramArgs, go_format_any.ToString(val))
			}
		} else {
			return ``, nil, errors.New(`map value type error`)
		}
		updateStr = strings.TrimSuffix(updateStr, ",")
	case reflect.Struct:
		map_ := make(map[string]any)
		err := mysql.structToMap(updateParams.Update, map_)
		if err != nil {
			return ``, nil, err
		}
		for key, val := range map_ {
			if val == nil {
				continue
			}
			updateStr += fmt.Sprintf("`%s` = ?,", key)
			paramArgs = append(paramArgs, go_format_any.ToString(val))
		}
		updateStr = strings.TrimSuffix(updateStr, ",")
	case reflect.String:
		updateStr = updateParams.Update.(string)
	default:
		return ``, nil, errors.New(`Type error.`)

	}

	paramArgsTemp, whereStr, err := mysql.buildWhere(updateParams.Where, remainValues)
	if err != nil {
		return ``, nil, err
	}

	paramArgs = append(paramArgs, paramArgsTemp...)

	str := fmt.Sprintf(
		"update `%s` set %s %s",
		updateParams.TableName,
		updateStr,
		whereStr,
	)
	return str, paramArgs, nil
}
