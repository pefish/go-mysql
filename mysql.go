package go_mysql

import (
	sql2 "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"text/template"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pefish/go-mysql/sqlx"
	"github.com/pefish/go-reflect"
	"github.com/satori/go.uuid"
)

type InterfaceLogger interface {
	Debug(args ...interface{})
	DebugF(format string, args ...interface{})
	Info(args ...interface{})
	InfoF(format string, args ...interface{})
	Warn(args ...interface{})
	WarnF(format string, args ...interface{})
	Error(args ...interface{})
	ErrorF(format string, args ...interface{})
}

type Configuration struct {
	Host            string
	Port            interface{}
	Username        string
	Password        string
	Database        interface{}
	MaxOpenConns    interface{}
	MaxIdleConns    interface{}
	ConnMaxLifetime interface{}
}

var (
	DEFAULT_PORT              uint64 = 3306
	DEFAULT_MAX_OPEN_CONNS    uint64 = 100
	DEFAULT_MAX_IDLE_CONNS    uint64 = 30
	DEFAULT_CONN_MAX_LIFTTIME        = 6 * time.Second
)

var MysqlHelper = MysqlClass{
	TagName: `json`,
}

// ----------------------------- MysqlClass -----------------------------

type MysqlClass struct {
	Db      *sqlx.DB
	TxId    string
	Tx      *sqlx.Tx
	TagName string
	Logger  InterfaceLogger
}

func (this *MysqlClass) SetLogger(logger InterfaceLogger) {
	this.Logger = logger
}

func (this *MysqlClass) Close() {
	if this.Db != nil {
		err := this.Db.Close()
		if err != nil {
			this.Logger.Error(err)
		} else {
			this.Logger.Info(`mysql close succeed.`)
		}
	}
	if this.Tx != nil {
		err := this.Tx.Rollback()
		if err != nil {
			this.Logger.Error(err)
		}
	}
}

func (this *MysqlClass) MustConnectWithConfiguration(configuration Configuration) {
	var port = DEFAULT_PORT
	if configuration.Port != nil {
		port = go_reflect.Reflect.MustToUint64(configuration.Port)
	}
	var database *string
	if configuration.Database != nil {
		d := go_reflect.Reflect.MustToString(configuration.Database)
		database = &d
	}
	var maxOpenConns = DEFAULT_MAX_OPEN_CONNS
	if configuration.MaxOpenConns != nil {
		maxOpenConns = go_reflect.Reflect.MustToUint64(configuration.MaxOpenConns)
	}
	var maxIdleConns = DEFAULT_MAX_IDLE_CONNS
	if configuration.MaxIdleConns != nil {
		maxIdleConns = go_reflect.Reflect.MustToUint64(configuration.MaxIdleConns)
	}
	connMaxLifetime := DEFAULT_CONN_MAX_LIFTTIME
	if configuration.ConnMaxLifetime != nil {
		connMaxLifetime = configuration.ConnMaxLifetime.(time.Duration)
	}

	this.MustConnect(configuration.Host, port, configuration.Username, configuration.Password, database, maxOpenConns, maxIdleConns, connMaxLifetime)
}

func (this *MysqlClass) MustConnectWithMap(map_ map[string]interface{}) {
	var port = DEFAULT_PORT
	if map_[`port`] != nil {
		port = go_reflect.Reflect.MustToUint64(map_[`port`])
	}
	var database *string
	if map_[`database`] != nil {
		d := go_reflect.Reflect.MustToString(map_[`database`])
		database = &d
	}
	var maxOpenConns = DEFAULT_MAX_OPEN_CONNS
	if map_[`maxOpenConns`] != nil {
		maxOpenConns = go_reflect.Reflect.MustToUint64(map_[`maxOpenConns`])
	}
	var maxIdleConns = DEFAULT_MAX_IDLE_CONNS
	if map_[`maxIdleConns`] != nil {
		maxIdleConns = go_reflect.Reflect.MustToUint64(map_[`maxIdleConns`])
	}
	connMaxLifetime := DEFAULT_CONN_MAX_LIFTTIME
	if map_[`connMaxLifeTime`] != nil {
		fmt.Println(reflect.TypeOf(map_[`connMaxLifeTime`]).Kind())
		connMaxLifetime = time.Duration(go_reflect.Reflect.MustToInt64(map_[`connMaxLifeTime`])) * time.Second
	}

	this.MustConnect(map_[`host`].(string), port, map_[`username`].(string), map_[`password`].(string), database, maxOpenConns, maxIdleConns, connMaxLifetime)
}

func (this *MysqlClass) MustConnect(host string, port uint64, username string, password string, database *string, maxOpenConns uint64, maxIdleConns uint64, connMaxLifetime time.Duration) {
	d := ``
	if database != nil {
		d = *database
	}
	address := fmt.Sprintf(`%s:%d`, host, port)
	this.Logger.Info(fmt.Sprintf(`mysql connecting... url: %s`, address))
	connUrl := fmt.Sprintf(
		`%s:%s@tcp(%s)/%s?charset=utf8&parseTime=true&multiStatements=true&loc=UTC`,
		username,
		password,
		address,
		d,
	)
	db := sqlx.MustConnect(`mysql`, connUrl)
	db.SetTagName(this.TagName)
	this.Logger.Info(fmt.Sprintf(`mysql connect succeed. url: %s`, address))
	db.DB.SetMaxOpenConns(int(maxOpenConns))  // 用于设置最大打开的连接数，默认值为0表示不限制
	db.DB.SetMaxIdleConns(int(maxIdleConns))  // 用于设置闲置的连接数
	db.DB.SetConnMaxLifetime(connMaxLifetime) // 设置一个超时时间，时间小于数据库的超时时间即可
	this.Db = db
}

func (this *MysqlClass) printDebugInfo(sql string, values interface{}) {
	txInfo := ``
	if this.Tx != nil {
		txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
	}
	this.Logger.DebugF(`%s%s, %v`, txInfo, sql, values)
}

func (this *MysqlClass) processValues(sql string, values []interface{}) (string, []interface{}, error) {
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
			return ``, nil, err
		}
	}
	return sql, values, nil
}

func (this *MysqlClass) MustRawExec(sql string, values ...interface{}) (uint64, uint64) {
	lastInsertId, rowsAffected, err := this.RawExec(sql, values...)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (this *MysqlClass) RawExec(sql string, values ...interface{}) (uint64, uint64, error) {
	sql, values, err := this.processValues(sql, values)
	if err != nil {
		return 0, 0, err
	}
	this.printDebugInfo(sql, values)

	var result sql2.Result
	if this.Tx != nil {
		result, err = this.Tx.Exec(sql, values...)
	} else {
		result, err = this.Db.Exec(sql, values...)
	}
	if err != nil {
		return 0, 0, err
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

func (this *MysqlClass) MustRawSelect(dest interface{}, sql string, values ...interface{}) {
	err := this.RawSelect(dest, sql, values...)
	if err != nil {
		panic(err)
	}
}

func (this *MysqlClass) RawSelect(dest interface{}, sql string, values ...interface{}) error {
	sql, values, err := this.processValues(sql, values)
	if err != nil {
		return err
	}
	this.printDebugInfo(sql, values)

	if this.Tx != nil {
		err = this.Tx.Select(dest, sql, values...)
	} else {
		err = this.Db.Select(dest, sql, values...)
	}
	if err != nil {
		return err
	}
	return nil
}

func (this *MysqlClass) MustCount(tableName string, args ...interface{}) uint64 {
	result, err := this.Count(tableName, args...)
	if err != nil {
		panic(err)
	}
	return result
}

func (this *MysqlClass) Count(tableName string, args ...interface{}) (uint64, error) {
	var countStruct struct {
		Count uint64 `json:"count"`
	}
	sql, paramArgs, err := Builder.BuildCountSql(tableName, args...)
	if err != nil {
		return 0, err
	}
	this.RawSelectFirst(&countStruct, sql, paramArgs...)
	return countStruct.Count, nil
}

func (this *MysqlClass) MustSum(tableName string, sumTarget string, args ...interface{}) string {
	result, err := this.Sum(tableName, sumTarget, args...)
	if err != nil {
		panic(err)
	}
	return result
}

func (this *MysqlClass) Sum(tableName string, sumTarget string, args ...interface{}) (string, error) {
	var sumStruct struct {
		Sum *string `json:"sum"`
	}
	sql, paramArgs, err := Builder.BuildSumSql(tableName, sumTarget, args...)
	if err != nil {
		return ``, err
	}
	this.RawSelectFirst(&sumStruct, sql, paramArgs...)
	if sumStruct.Sum == nil {
		return `0`, nil
	}
	return *sumStruct.Sum, nil
}

func (this *MysqlClass) MustSelectFirst(dest interface{}, tableName string, select_ string, args ...interface{}) bool {
	bool_, err := this.SelectFirst(dest, tableName, select_, args...)
	if err != nil {
		panic(err)
	}
	return bool_
}

func (this *MysqlClass) SelectFirst(dest interface{}, tableName string, select_ string, args ...interface{}) (bool, error) {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, this.TagName), `,`)
	}
	sql, paramArgs, err := Builder.BuildSelectSql(tableName, select_, args...)
	if err != nil {
		return true, err
	}
	return this.RawSelectFirst(dest, sql, paramArgs...)
}

func (this *MysqlClass) MustSelectFirstByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) bool {
	bool_, err := this.SelectFirstByStr(dest, tableName, select_, str, values...)
	if err != nil {
		panic(err)
	}
	return bool_
}

func (this *MysqlClass) SelectFirstByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) (bool, error) {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, this.TagName), `,`)
	}
	sql := fmt.Sprintf(
		`select %s from %s %s`,
		select_,
		tableName,
		str,
	)
	return this.RawSelectFirst(dest, sql, values...)
}

func (this *MysqlClass) MustSelectById(dest interface{}, tableName string, select_ string, id uint64, forUpdate bool) bool {
	bool_, err := this.SelectById(dest, tableName, select_, id, forUpdate)
	if err != nil {
		panic(err)
	}
	return bool_
}

func (this *MysqlClass) SelectById(dest interface{}, tableName string, select_ string, id uint64, forUpdate bool) (notFound bool, err error) {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, this.TagName), `,`)
	}
	var paramArgs = []interface{}{}
	sql, paramArgs, err := Builder.BuildSelectSql(tableName, select_, map[string]interface{}{
		`id`: id,
	}, nil, nil, forUpdate)
	if err != nil {
		return true, err
	}
	return this.RawSelectFirst(dest, sql, paramArgs...)
}

func (this *MysqlClass) MustSelect(dest interface{}, tableName string, select_ string, args ...interface{}) {
	err := this.Select(dest, tableName, select_, args...)
	if err != nil {
		panic(err)
	}
}

func (this *MysqlClass) Select(dest interface{}, tableName string, select_ string, args ...interface{}) error {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, this.TagName), `,`)
	}
	var paramArgs = []interface{}{}
	sql, paramArgs, err := Builder.BuildSelectSql(tableName, select_, args...)
	if err != nil {
		return err
	}
	this.RawSelect(dest, sql, paramArgs...)
	return nil
}

func (this *MysqlClass) SelectByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, this.TagName), `,`)
	}
	sql := fmt.Sprintf(
		`select %s from %s %s`,
		select_,
		tableName,
		str,
	)
	this.RawSelect(dest, sql, values...)
}

func (this *MysqlClass) MustAffectedInsert(tableName string, params interface{}) (lastInsertId uint64) {
	lastInsertId, rowsAffected, err := this.Insert(tableName, params)
	if err != nil {
		panic(err)
	}
	if rowsAffected == 0 {
		panic(errors.New(`no affected`))
	}
	return lastInsertId
}

func (this *MysqlClass) MustInsert(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64) {
	lastInsertId, rowsAffected, err := this.Insert(tableName, params)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (this *MysqlClass) Insert(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := Builder.BuildInsertSql(tableName, params, BuildInsertSqlOpt{})
	if err != nil {
		return 0, 0, err
	}
	return this.RawExec(sql, paramArgs...)
}

func (this *MysqlClass) MustInsertIgnore(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64) {
	lastInsertId, rowsAffected, err := this.InsertIgnore(tableName, params)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (this *MysqlClass) InsertIgnore(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := Builder.BuildInsertSql(tableName, params, BuildInsertSqlOpt{
		InsertIgnore: true,
	})
	if err != nil {
		return 0, 0, err
	}
	return this.RawExec(sql, paramArgs...)
}

func (this *MysqlClass) MustReplaceInto(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64) {
	lastInsertId, rowsAffected, err := this.ReplaceInto(tableName, params)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (this *MysqlClass) ReplaceInto(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := Builder.BuildInsertSql(tableName, params, BuildInsertSqlOpt{
		ReplaceInto: true,
	})
	if err != nil {
		return 0, 0, err
	}
	return this.RawExec(sql, paramArgs...)
}

func (this *MysqlClass) MustUpdate(tableName string, update interface{}, args ...interface{}) (lastInsertId uint64, rowsAffected uint64) {
	lastInsertId, rowsAffected, err := this.Update(tableName, update, args...)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (this *MysqlClass) Update(tableName string, update interface{}, args ...interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := Builder.BuildUpdateSql(tableName, update, args...)
	if err != nil {
		return 0, 0, err
	}
	return this.RawExec(sql, paramArgs...)
}

func (this *MysqlClass) MustAffectedUpdate(tableName string, update interface{}, args ...interface{}) (lastInsertId uint64) {
	lastInsertId, rowsAffected, err := this.Update(tableName, update, args...)
	if err != nil {
		panic(err)
	}
	if rowsAffected == 0 {
		panic(errors.New(`no affected`))
	}
	return lastInsertId
}

func (this *MysqlClass) MustRawSelectFirst(dest interface{}, sql string, values ...interface{}) bool {
	notFound, err := this.RawSelectFirst(dest, sql, values...)
	if err != nil {
		panic(err)
	}
	return notFound
}

func (this *MysqlClass) RawSelectFirst(dest interface{}, sql string, values ...interface{}) (bool, error) {
	sql, values, err := this.processValues(sql, values)
	if err != nil {
		return true, err
	}
	this.printDebugInfo(sql, values)

	if this.Tx != nil {
		err = this.Tx.Get(dest, sql, values...)
	} else {
		err = this.Db.Get(dest, sql, values...)
	}
	if err != nil {
		if err.Error() == `sql: no rows in result set` {
			return true, nil
		} else {
			return true, err
		}
	}

	return false, nil
}

func (this *MysqlClass) MustBegin() *MysqlClass {
	c, err := this.Begin()
	if err != nil {
		panic(err)
	}
	return c
}

func (this *MysqlClass) Begin() (*MysqlClass, error) {
	id := fmt.Sprintf(`%s`, uuid.NewV4())
	this.printDebugInfo(`begin`, nil)
	tx, err := this.Db.Beginx()
	if err != nil {
		return nil, err
	}
	return &MysqlClass{
		Db:      nil,
		TxId:    id,
		Tx:      tx,
		TagName: this.TagName,
		Logger: this.Logger,
	}, nil
}

func (this *MysqlClass) MustCommit() {
	err := this.Commit()
	if err != nil {
		panic(err)
	}
}

func (this *MysqlClass) Commit() error {
	this.printDebugInfo(`commit`, nil)

	err := this.Tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (this *MysqlClass) MustRollback() {
	err := this.Rollback()
	if err != nil {
		panic(err)
	}
}

func (this *MysqlClass) Rollback() error {
	this.printDebugInfo(`rollback`, nil)

	err := this.Tx.Rollback()
	if err != nil {
		return err
	}
	return nil
}

func (this *MysqlClass) RollbackWithErr() error {
	this.printDebugInfo(`rollback`, nil)

	return this.Tx.Rollback()
}

// ----------------------------- BuilderClass -----------------------------

type BuilderClass struct {
}

var Builder = BuilderClass{}

type BuildInsertSqlOpt struct {
	InsertIgnore bool
	ReplaceInto  bool
}

func (this *BuilderClass) MustBuildInsertSql(tableName string, params interface{}, opt BuildInsertSqlOpt) (string, []interface{}) {
	str, paramArgs, err := this.BuildInsertSql(tableName, params, opt)
	if err != nil {
		panic(err)
	}
	return str, paramArgs
}

func (this *BuilderClass) BuildInsertSql(tableName string, params interface{}, opt BuildInsertSqlOpt) (string, []interface{}, error) {
	var cols []string
	var vals []string
	var paramArgs = []interface{}{}
	type_ := reflect.TypeOf(params)
	kind := type_.Kind()
	if kind == reflect.Map {
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			for key, val := range params.(map[string]interface{}) {
				if val == nil {
					continue
				}
				cols = append(cols, key)
				vals = append(vals, `?`)
				str, err := go_reflect.Reflect.ToString(val)
				if err != nil {
					return ``, nil, err
				}
				paramArgs = append(paramArgs, template.HTMLEscapeString(str))
			}
		} else {
			return ``, nil, errors.New(`map value type error`)
		}
	} else if kind == reflect.Struct {
		map_, err := this.structToMap(params)
		if err != nil {
			return ``, nil, err
		}
		for key, val := range map_ {
			if val == nil {
				continue
			}
			cols = append(cols, key)
			vals = append(vals, `?`)
			str, err := go_reflect.Reflect.ToString(val)
			if err != nil {
				return ``, nil, err
			}
			paramArgs = append(paramArgs, template.HTMLEscapeString(str))
		}
	} else {
		return ``, nil, errors.New(`type error`)
	}

	insertStr := `insert`
	if opt.InsertIgnore == true {
		insertStr += ` ignore`
	} else if opt.ReplaceInto == true {
		insertStr = `replace into`
	}
	str := fmt.Sprintf(
		`%s into %s (%s) values (%s)`,
		insertStr,
		tableName,
		strings.Join(cols, `,`),
		strings.Join(vals, `,`),
	)
	return str, paramArgs, nil
}

func (this *BuilderClass) MustBuildCountSql(tableName string, args ...interface{}) (string, []interface{}) {
	paramArgs, whereStr, err := this.BuildCountSql(tableName, args...)
	if err != nil {
		panic(err)
	}
	return paramArgs, whereStr
}

func (this *BuilderClass) BuildCountSql(tableName string, args ...interface{}) (string, []interface{}, error) {
	var whereStr = ``
	var paramArgs = []interface{}{}
	if len(args) > 0 && args[0] != nil {
		var err error
		paramArgs, whereStr, err = this.BuildWhere(args[0])
		if err != nil {
			return ``, nil, err
		}
	}

	str := fmt.Sprintf(
		`select count(*) as count from %s %s`,
		tableName,
		whereStr,
	)
	return str, paramArgs, nil
}

func (this *BuilderClass) MustBuildSumSql(tableName string, sumTarget string, args ...interface{}) (string, []interface{}) {
	str, paramArgs, err := this.BuildSumSql(tableName, sumTarget, args...)
	if err != nil {
		panic(err)
	}
	return str, paramArgs
}

func (this *BuilderClass) BuildSumSql(tableName string, sumTarget string, args ...interface{}) (string, []interface{}, error) {
	var whereStr = ``
	var paramArgs = []interface{}{}
	if len(args) > 0 && args[0] != nil {
		var err error
		paramArgs, whereStr, err = this.BuildWhere(args[0])
		if err != nil {
			return ``, nil, err
		}
	}

	str := fmt.Sprintf(
		`select sum(%s) as sum from %s %s`,
		sumTarget,
		tableName,
		whereStr,
	)
	return str, paramArgs, nil
}

func (this *BuilderClass) buildWhereFromMapInterface(ele map[string]interface{}) ([]interface{}, string, error) {
	andStr := ``
	tempParamArgs := []interface{}{}
	for key, val := range ele {
		if val == nil {
			continue
		}
		kind := reflect.TypeOf(val).Kind()
		if kind == reflect.Slice {
			val_ := val.([]interface{})
			str, err := go_reflect.Reflect.ToString(val_[0])
			if err != nil {
				return nil, ``, err
			}
			andStr = andStr + key + ` ` + str + ` ? and `
			str, err = go_reflect.Reflect.ToString(val_[1])
			if err != nil {
				return nil, ``, err
			}
			tempParamArgs = append(tempParamArgs, template.HTMLEscapeString(str))
		} else {
			str, err := go_reflect.Reflect.ToString(val)
			if err != nil {
				return nil, ``, err
			}
			valStr := template.HTMLEscapeString(str)
			andStr = andStr + key + ` = ? and `
			tempParamArgs = append(tempParamArgs, valStr)
		}
	}
	if len(andStr) > 4 {
		andStr = andStr[:len(andStr)-5]
	}
	return tempParamArgs, andStr, nil
}

func (this *BuilderClass) MustBuildWhere(where interface{}) ([]interface{}, string) {
	paramArgs, str, err := this.BuildWhere(where)
	if err != nil {
		panic(err)
	}
	return paramArgs, str
}

func (this *BuilderClass) BuildWhere(where interface{}) ([]interface{}, string, error) {
	whereStr := `where `
	type_ := reflect.TypeOf(where)
	kind := type_.Kind()
	paramArgs := []interface{}{}
	if kind == reflect.String {
		return paramArgs, where.(string), nil
	}
	str := ``
	if kind == reflect.Map {
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			var err error
			paramArgs, str, err = this.buildWhereFromMapInterface(where.(map[string]interface{}))
			if err != nil {
				return nil, ``, err
			}
		} else {
			return nil, ``, errors.New(`map value type error`)
		}
	} else if kind == reflect.Struct {
		map_, err := this.structToMap(where)
		if err != nil {
			return nil, ``, err
		}
		paramArgs, str, err = this.buildWhereFromMapInterface(map_)
		if err != nil {
			return nil, ``, err
		}
	} else if kind == reflect.Slice { // or
		if type_.Elem().Kind() != reflect.Map {
			return nil, ``, errors.New(`slice value type error`)
		}
		mapKind := type_.Elem().Elem().Kind()
		if mapKind == reflect.Interface {
			sliceVal := where.([]map[string]interface{})
			for _, ele := range sliceVal {
				paramArgsTemp, str, err := this.buildWhereFromMapInterface(ele)
				if err != nil {
					return nil, ``, err
				}
				paramArgs = append(paramArgs, paramArgsTemp...)
				whereStr += `(` + str + `) or `
			}
		} else {
			return nil, ``, errors.New(`map value type error`)
		}
		if len(whereStr) > 3 {
			whereStr = whereStr[:len(whereStr)-4]
		}
	} else {
		return nil, ``, errors.New(`where type error`)
	}
	return paramArgs, whereStr + str, nil
}

func (this *BuilderClass) MustBuildSelectSql(tableName string, select_ string, args ...interface{}) (string, []interface{}) {
	str, paramArgs, err := this.BuildSelectSql(tableName, select_, args...)
	if err != nil {
		panic(err)
	}
	return str, paramArgs
}

func (this *BuilderClass) BuildSelectSql(tableName string, select_ string, args ...interface{}) (string, []interface{}, error) {
	var whereStr = ``
	var paramArgs = []interface{}{}
	if len(args) > 0 && args[0] != nil {
		var err error
		paramArgs, whereStr, err = this.BuildWhere(args[0])
		if err != nil {
			return ``, nil, err
		}
	}

	orderByStr := ``
	if len(args) > 1 && args[1] != nil {
		orderByStr = args[1].(string)
	}

	limitStr := ``
	if len(args) > 2 && args[2] != nil {
		limitStr = args[2].(string)
	}

	forUpdateStr := ``
	if len(args) > 3 && args[3] == true {
		forUpdateStr = `for update`
	}

	str := fmt.Sprintf(
		`select %s from %s %s %s %s %s`,
		select_,
		tableName,
		whereStr,
		orderByStr,
		limitStr,
		forUpdateStr,
	)
	return str, paramArgs, nil
}

func (this *BuilderClass) structToMap(in_ interface{}) (map[string]interface{}, error) {
	var result map[string]interface{}
	inrec, err := json.Marshal(in_)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(inrec, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (this *BuilderClass) MustBuildUpdateSql(tableName string, update interface{}, args ...interface{}) (string, []interface{}) {
	str, paramArgs, err := this.BuildUpdateSql(tableName, update, args...)
	if err != nil {
		panic(err)
	}
	return str, paramArgs
}

func (this *BuilderClass) BuildUpdateSql(tableName string, update interface{}, args ...interface{}) (string, []interface{}, error) {
	var updateStr = ``
	var paramArgs = []interface{}{}
	type_ := reflect.TypeOf(update)
	updateKind := type_.Kind()
	if updateKind == reflect.Map {
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			for key, val := range update.(map[string]interface{}) {
				if val == nil {
					continue
				}
				updateStr = updateStr + key + ` = ?,`
				str, err := go_reflect.Reflect.ToString(val)
				if err != nil {
					return ``, nil, err
				}
				paramArgs = append(paramArgs, template.HTMLEscapeString(str))
			}
		} else {
			return ``, nil, errors.New(`map value type error`)
		}
	} else if updateKind == reflect.Struct {
		map_, err := this.structToMap(update)
		if err != nil {
			return ``, nil, err
		}
		for key, val := range map_ {
			if val == nil {
				continue
			}
			updateStr = updateStr + key + ` = ?,`
			str, err := go_reflect.Reflect.ToString(val)
			if err != nil {
				return ``, nil, err
			}
			paramArgs = append(paramArgs, template.HTMLEscapeString(str))
		}
	} else {
		return ``, nil, errors.New(`type error`)
	}
	if len(updateStr) > 0 {
		updateStr = updateStr[:len(updateStr)-1]
	}

	var whereStr = ``
	if len(args) > 0 && args[0] != nil {
		paramArgsTemp, whereStrTemp, err := this.BuildWhere(args[0])
		if err != nil {
			return ``, nil, err
		}
		paramArgs = append(paramArgs, paramArgsTemp...)
		whereStr = whereStrTemp
	}

	str := fmt.Sprintf(
		`update %s set %s %s`,
		tableName,
		updateStr,
		whereStr,
	)
	return str, paramArgs, nil
}
