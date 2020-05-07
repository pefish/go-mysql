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
	go_interface_logger "github.com/pefish/go-interface-logger"
	"github.com/pefish/go-mysql/sqlx"
	"github.com/pefish/go-reflect"
	"github.com/satori/go.uuid"
)

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
	Logger:  go_interface_logger.DefaultLogger,
}

// ----------------------------- MysqlClass -----------------------------

type MysqlClass struct {
	Db      *sqlx.DB
	TxId    string
	Tx      *sqlx.Tx
	TagName string
	Logger  go_interface_logger.InterfaceLogger
}

func (mysql *MysqlClass) SetLogger(logger go_interface_logger.InterfaceLogger) {
	mysql.Logger = logger
}

func (mysql *MysqlClass) Close() {
	if mysql.Db != nil {
		err := mysql.Db.Close()
		if err != nil {
			mysql.Logger.Error(err)
		} else {
			mysql.Logger.Info(`mysql close succeed.`)
		}
	}
	if mysql.Tx != nil {
		err := mysql.Tx.Rollback()
		if err != nil {
			mysql.Logger.Error(err)
		}
	}
}

func (mysql *MysqlClass) MustConnectWithConfiguration(configuration Configuration) {
	err := mysql.ConnectWithConfiguration(configuration)
	if err != nil {
		panic(err)
	}
}

func (mysql *MysqlClass) ConnectWithConfiguration(configuration Configuration) error {
	var port = DEFAULT_PORT
	if configuration.Port != nil {
		port_, err := go_reflect.Reflect.ToUint64(configuration.Port)
		if err != nil {
			return err
		}
		port = port_
	}
	var database *string
	if configuration.Database != nil {
		d, err := go_reflect.Reflect.ToString(configuration.Database)
		if err != nil {
			return err
		}
		database = &d
	}
	var maxOpenConns = DEFAULT_MAX_OPEN_CONNS
	if configuration.MaxOpenConns != nil {
		maxOpenConns_, err := go_reflect.Reflect.ToUint64(configuration.MaxOpenConns)
		if err != nil {
			return err
		}
		maxOpenConns = maxOpenConns_
	}
	var maxIdleConns = DEFAULT_MAX_IDLE_CONNS
	if configuration.MaxIdleConns != nil {
		maxIdleConns_, err := go_reflect.Reflect.ToUint64(configuration.MaxIdleConns)
		if err != nil {
			return err
		}
		maxIdleConns = maxIdleConns_
	}
	connMaxLifetime := DEFAULT_CONN_MAX_LIFTTIME
	if configuration.ConnMaxLifetime != nil {
		connMaxLifetime = configuration.ConnMaxLifetime.(time.Duration)
	}

	err := mysql.Connect(configuration.Host, port, configuration.Username, configuration.Password, database, maxOpenConns, maxIdleConns, connMaxLifetime)
	if err != nil {
		return err
	}
	return nil
}

func (mysql *MysqlClass) MustConnectWithMap(map_ map[string]interface{}) {
	err := mysql.ConnectWithMap(map_)
	if err != nil {
		panic(err)
	}
}

func (mysql *MysqlClass) ConnectWithMap(map_ map[string]interface{}) error {
	var port = DEFAULT_PORT
	if map_[`port`] != nil {
		port_, err := go_reflect.Reflect.ToUint64(map_[`port`])
		if err != nil {
			return err
		}
		port = port_
	}
	var database *string
	if map_[`database`] != nil {
		d, err := go_reflect.Reflect.ToString(map_[`database`])
		if err != nil {
			return err
		}
		database = &d
	}
	var maxOpenConns = DEFAULT_MAX_OPEN_CONNS
	if map_[`maxOpenConns`] != nil {
		maxOpenConns_, err := go_reflect.Reflect.ToUint64(map_[`maxOpenConns`])
		if err != nil {
			return err
		}
		maxOpenConns = maxOpenConns_
	}
	var maxIdleConns = DEFAULT_MAX_IDLE_CONNS
	if map_[`maxIdleConns`] != nil {
		maxIdleConns_, err := go_reflect.Reflect.ToUint64(map_[`maxIdleConns`])
		if err != nil {
			return err
		}
		maxIdleConns = maxIdleConns_
	}
	connMaxLifetime := DEFAULT_CONN_MAX_LIFTTIME
	if map_[`connMaxLifeTime`] != nil {
		connMaxLifeTime_, err := go_reflect.Reflect.ToInt64(map_[`connMaxLifeTime`])
		if err != nil {
			return err
		}
		connMaxLifetime = time.Duration(connMaxLifeTime_) * time.Second
	}

	err := mysql.Connect(map_[`host`].(string), port, map_[`username`].(string), map_[`password`].(string), database, maxOpenConns, maxIdleConns, connMaxLifetime)
	if err != nil {
		return err
	}
	return nil
}

func (mysql *MysqlClass) Connect(host string, port uint64, username string, password string, database *string, maxOpenConns uint64, maxIdleConns uint64, connMaxLifetime time.Duration) error {
	d := ``
	if database != nil {
		d = *database
	}
	address := fmt.Sprintf(`%s:%d`, host, port)
	mysql.Logger.Info(fmt.Sprintf(`mysql connecting... url: %s`, address))
	connUrl := fmt.Sprintf(
		`%s:%s@tcp(%s)/%s?charset=utf8&parseTime=true&multiStatements=true&loc=UTC`,
		username,
		password,
		address,
		d,
	)
	db, err := sqlx.Connect(`mysql`, connUrl)
	if err != nil {
		return err
	}
	db.SetTagName(mysql.TagName)
	mysql.Logger.Info(fmt.Sprintf(`mysql connect succeed. url: %s`, address))
	db.DB.SetMaxOpenConns(int(maxOpenConns))  // 用于设置最大打开的连接数，默认值为0表示不限制
	db.DB.SetMaxIdleConns(int(maxIdleConns))  // 用于设置闲置的连接数
	db.DB.SetConnMaxLifetime(connMaxLifetime) // 设置一个超时时间，时间小于数据库的超时时间即可
	mysql.Db = db

	return nil
}

func (mysql *MysqlClass) printDebugInfo(sql string, values interface{}, printInfo bool) {
	txInfo := ``
	if mysql.Tx != nil {
		txInfo = fmt.Sprintf(`[transaction id: %s] `, mysql.TxId)
	}
	if printInfo {
		mysql.Logger.InfoF(`%s%s, %v`, txInfo, sql, values)
	} else {
		mysql.Logger.DebugF(`%s%s, %v`, txInfo, sql, values)
	}
}

func (mysql *MysqlClass) MustRawSelectByStr(dest interface{}, select_ string, str string, values ...interface{}) {
	err := mysql.RawSelectByStr(dest, select_, str, values...)
	if err != nil {
		panic(err)
	}
}

func (mysql *MysqlClass) RawSelectByStr(dest interface{}, select_ string, str string, values ...interface{}) error {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mysql.TagName), `,`)
	}
	sql := fmt.Sprintf(
		`select %s %s`,
		select_,
		str,
	)
	err := mysql.RawSelect(dest, sql, values...)
	if err != nil {
		return err
	}
	return nil
}

func (mysql *MysqlClass) processValues(sql string, values []interface{}) (string, []interface{}, error) {
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

func (mysql *MysqlClass) MustRawExec(sql string, values ...interface{}) (uint64, uint64) {
	lastInsertId, rowsAffected, err := mysql.RawExec(sql, values...)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (mysql *MysqlClass) RawExec(sql string, values ...interface{}) (uint64, uint64, error) {
	sql, values, err := mysql.processValues(sql, values)
	mysql.printDebugInfo(sql, values, true)
	if err != nil {
		return 0, 0, err
	}

	var result sql2.Result
	if mysql.Tx != nil {
		result, err = mysql.Tx.Exec(sql, values...)
	} else {
		result, err = mysql.Db.Exec(sql, values...)
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

func (mysql *MysqlClass) MustRawSelect(dest interface{}, sql string, values ...interface{}) {
	err := mysql.RawSelect(dest, sql, values...)
	if err != nil {
		panic(err)
	}
}

func (mysql *MysqlClass) RawSelect(dest interface{}, sql string, values ...interface{}) error {
	if strings.HasPrefix(sql, `select *`) { // 只支持固定前缀的select *
		sql = `select ` + strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mysql.TagName), `,`) + sql[8:]
	}
	sql, values, err := mysql.processValues(sql, values)
	mysql.printDebugInfo(sql, values, false)
	if err != nil {
		return err
	}
	if mysql.Tx != nil {
		err = mysql.Tx.Select(dest, sql, values...)
	} else {
		err = mysql.Db.Select(dest, sql, values...)
	}
	if err != nil {
		return err
	}
	return nil
}

func (mysql *MysqlClass) MustCount(tableName string, args ...interface{}) uint64 {
	result, err := mysql.Count(tableName, args...)
	if err != nil {
		panic(err)
	}
	return result
}

func (mysql *MysqlClass) Count(tableName string, args ...interface{}) (uint64, error) {
	var countStruct struct {
		Count uint64 `json:"count"`
	}
	sql, paramArgs, err := Builder.BuildCountSql(tableName, args...)
	if err != nil {
		return 0, err
	}
	_, err = mysql.RawSelectFirst(&countStruct, sql, paramArgs...)
	if err != nil {
		return 0, err
	}
	return countStruct.Count, nil
}

func (mysql *MysqlClass) MustSum(tableName string, sumTarget string, args ...interface{}) string {
	result, err := mysql.Sum(tableName, sumTarget, args...)
	if err != nil {
		panic(err)
	}
	return result
}

func (mysql *MysqlClass) Sum(tableName string, sumTarget string, args ...interface{}) (string, error) {
	var sumStruct struct {
		Sum *string `json:"sum"`
	}
	sql, paramArgs, err := Builder.BuildSumSql(tableName, sumTarget, args...)
	if err != nil {
		return ``, err
	}
	_, err = mysql.RawSelectFirst(&sumStruct, sql, paramArgs...)
	if err != nil {
		return ``, err
	}
	if sumStruct.Sum == nil {
		return `0`, nil
	}
	return *sumStruct.Sum, nil
}

func (mysql *MysqlClass) MustSelectFirst(dest interface{}, tableName string, select_ string, args ...interface{}) bool {
	bool_, err := mysql.SelectFirst(dest, tableName, select_, args...)
	if err != nil {
		panic(err)
	}
	return bool_
}

func (mysql *MysqlClass) SelectFirst(dest interface{}, tableName string, select_ string, args ...interface{}) (bool, error) {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mysql.TagName), `,`)
	}
	sql, paramArgs, err := Builder.BuildSelectSql(tableName, select_, args...)
	if err != nil {
		return true, err
	}
	return mysql.RawSelectFirst(dest, sql, paramArgs...)
}

func (mysql *MysqlClass) SelectFieldStrFirst(fieldName string, tableName string, args ...interface{}) (bool, *string, error) {
	sql, paramArgs, err := Builder.BuildSelectSql(tableName, fmt.Sprintf("%s as target", fieldName), args...)
	if err != nil {
		return true, nil, err
	}
	var targetStruct struct {
		Target *string `json:"target"`
	}
	notFound, err := mysql.RawSelectFirst(&targetStruct, sql, paramArgs...)
	if err != nil || notFound {
		return notFound, nil, err
	}
	return false, targetStruct.Target, nil
}

func (mysql *MysqlClass) MustSelectFirstByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) bool {
	bool_, err := mysql.SelectFirstByStr(dest, tableName, select_, str, values...)
	if err != nil {
		panic(err)
	}
	return bool_
}

func (mysql *MysqlClass) SelectFirstByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) (bool, error) {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mysql.TagName), `,`)
	}
	sql := fmt.Sprintf(
		`select %s from %s %s`,
		select_,
		tableName,
		str,
	)
	return mysql.RawSelectFirst(dest, sql, values...)
}

func (mysql *MysqlClass) MustSelectById(dest interface{}, tableName string, select_ string, id uint64, forUpdate bool) bool {
	bool_, err := mysql.SelectById(dest, tableName, select_, id, forUpdate)
	if err != nil {
		panic(err)
	}
	return bool_
}

func (mysql *MysqlClass) SelectById(dest interface{}, tableName string, select_ string, id uint64, forUpdate bool) (notFound bool, err error) {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mysql.TagName), `,`)
	}
	var paramArgs = []interface{}{}
	sql, paramArgs, err := Builder.BuildSelectSql(tableName, select_, map[string]interface{}{
		`id`: id,
	}, nil, nil, forUpdate)
	if err != nil {
		return true, err
	}
	return mysql.RawSelectFirst(dest, sql, paramArgs...)
}

func (mysql *MysqlClass) MustSelect(dest interface{}, tableName string, select_ string, args ...interface{}) {
	err := mysql.Select(dest, tableName, select_, args...)
	if err != nil {
		panic(err)
	}
}

func (mysql *MysqlClass) Select(dest interface{}, tableName string, select_ string, args ...interface{}) error {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mysql.TagName), `,`)
	}
	var paramArgs = []interface{}{}
	sql, paramArgs, err := Builder.BuildSelectSql(tableName, select_, args...)
	if err != nil {
		return err
	}
	err = mysql.RawSelect(dest, sql, paramArgs...)
	if err != nil {
		return err
	}
	return nil
}

func (mysql *MysqlClass) MustSelectByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) {
	err := mysql.SelectByStr(dest, tableName, select_, str, values...)
	if err != nil {
		panic(err)
	}
}

func (mysql *MysqlClass) SelectByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) error {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mysql.TagName), `,`)
	}
	sql := fmt.Sprintf(
		`select %s from %s %s`,
		select_,
		tableName,
		str,
	)
	err := mysql.RawSelect(dest, sql, values...)
	if err != nil {
		return err
	}
	return nil
}

func (mysql *MysqlClass) MustAffectedInsert(tableName string, params interface{}) (lastInsertId uint64) {
	lastInsertId, rowsAffected, err := mysql.Insert(tableName, params)
	if err != nil {
		panic(err)
	}
	if rowsAffected == 0 {
		panic(errors.New(`no affected`))
	}
	return lastInsertId
}

func (mysql *MysqlClass) MustInsert(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64) {
	lastInsertId, rowsAffected, err := mysql.Insert(tableName, params)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (mysql *MysqlClass) Insert(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := Builder.BuildInsertSql(tableName, params, BuildInsertSqlOpt{})
	if err != nil {
		return 0, 0, err
	}
	return mysql.RawExec(sql, paramArgs...)
}

func (mysql *MysqlClass) MustInsertIgnore(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64) {
	lastInsertId, rowsAffected, err := mysql.InsertIgnore(tableName, params)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (mysql *MysqlClass) InsertIgnore(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := Builder.BuildInsertSql(tableName, params, BuildInsertSqlOpt{
		InsertIgnore: true,
	})
	if err != nil {
		return 0, 0, err
	}
	return mysql.RawExec(sql, paramArgs...)
}

func (mysql *MysqlClass) InsertOnDuplicateKeyUpdate(tableName string, update map[string]interface{}, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := Builder.BuildInsertSql(tableName, params, BuildInsertSqlOpt{
		OnDuplicateKeyUpdate: update,
	})
	if err != nil {
		return 0, 0, err
	}
	return mysql.RawExec(sql, paramArgs...)
}

func (mysql *MysqlClass) MustReplaceInto(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64) {
	lastInsertId, rowsAffected, err := mysql.ReplaceInto(tableName, params)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (mysql *MysqlClass) ReplaceInto(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := Builder.BuildInsertSql(tableName, params, BuildInsertSqlOpt{
		ReplaceInto: true,
	})
	if err != nil {
		return 0, 0, err
	}
	return mysql.RawExec(sql, paramArgs...)
}

func (mysql *MysqlClass) MustUpdate(tableName string, update interface{}, args ...interface{}) (lastInsertId uint64, rowsAffected uint64) {
	lastInsertId, rowsAffected, err := mysql.Update(tableName, update, args...)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (mysql *MysqlClass) Update(tableName string, update interface{}, args ...interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := Builder.BuildUpdateSql(tableName, update, args...)
	if err != nil {
		return 0, 0, err
	}
	return mysql.RawExec(sql, paramArgs...)
}

func (mysql *MysqlClass) MustAffectedUpdate(tableName string, update interface{}, args ...interface{}) (lastInsertId uint64) {
	lastInsertId, rowsAffected, err := mysql.Update(tableName, update, args...)
	if err != nil {
		panic(err)
	}
	if rowsAffected == 0 {
		panic(errors.New(`no affected`))
	}
	return lastInsertId
}

func (mysql *MysqlClass) MustRawSelectFirst(dest interface{}, sql string, values ...interface{}) bool {
	notFound, err := mysql.RawSelectFirst(dest, sql, values...)
	if err != nil {
		panic(err)
	}
	return notFound
}

func (mysql *MysqlClass) RawSelectFirst(dest interface{}, sql string, values ...interface{}) (bool, error) {
	sql, values, err := mysql.processValues(sql, values)
	mysql.printDebugInfo(sql, values, false)
	if err != nil {
		return true, err
	}

	if mysql.Tx != nil {
		err = mysql.Tx.Get(dest, sql, values...)
	} else {
		err = mysql.Db.Get(dest, sql, values...)
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

func (mysql *MysqlClass) MustBegin() *MysqlClass {
	c, err := mysql.Begin()
	if err != nil {
		panic(err)
	}
	return c
}

func (mysql *MysqlClass) Begin() (*MysqlClass, error) {
	id := fmt.Sprintf(`%s`, uuid.NewV4())
	mysql.printDebugInfo(`begin`, nil, true)
	tx, err := mysql.Db.Beginx()
	if err != nil {
		return nil, err
	}
	return &MysqlClass{
		Db:      nil,
		TxId:    id,
		Tx:      tx,
		TagName: mysql.TagName,
		Logger:  mysql.Logger,
	}, nil
}

func (mysql *MysqlClass) MustCommit() {
	err := mysql.Commit()
	if err != nil {
		panic(err)
	}
}

func (mysql *MysqlClass) Commit() error {
	mysql.printDebugInfo(`commit`, nil, true)

	err := mysql.Tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (mysql *MysqlClass) MustRollback() {
	err := mysql.Rollback()
	if err != nil {
		panic(err)
	}
}

func (mysql *MysqlClass) Rollback() error {
	mysql.printDebugInfo(`rollback`, nil, true)

	err := mysql.Tx.Rollback()
	if err != nil {
		return err
	}
	return nil
}

func (mysql *MysqlClass) RollbackWithErr() error {
	mysql.printDebugInfo(`rollback`, nil, true)

	return mysql.Tx.Rollback()
}

// ----------------------------- BuilderClass -----------------------------

type BuilderClass struct {
}

var Builder = BuilderClass{}

type BuildInsertSqlOpt struct {
	InsertIgnore         bool
	ReplaceInto          bool
	OnDuplicateKeyUpdate map[string]interface{}
}

func (mysql *BuilderClass) MustBuildInsertSql(tableName string, params interface{}, opt BuildInsertSqlOpt) (string, []interface{}) {
	str, paramArgs, err := mysql.BuildInsertSql(tableName, params, opt)
	if err != nil {
		panic(err)
	}
	return str, paramArgs
}

func (mysql *BuilderClass) BuildInsertSql(tableName string, params interface{}, opt BuildInsertSqlOpt) (string, []interface{}, error) {
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
		map_, err := mysql.structToMap(params)
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
	if opt.OnDuplicateKeyUpdate != nil {
		str += " on duplicate key update "
		for key, val := range opt.OnDuplicateKeyUpdate {
			if val == nil {
				continue
			}
			str += key + ` = ?,`
			valStr, err := go_reflect.Reflect.ToString(val)
			if err != nil {
				return ``, nil, err
			}
			paramArgs = append(paramArgs, template.HTMLEscapeString(valStr))
		}
		str = strings.TrimSuffix(str, ",")
	}
	return str, paramArgs, nil
}

func (mysql *BuilderClass) MustBuildCountSql(tableName string, args ...interface{}) (string, []interface{}) {
	paramArgs, whereStr, err := mysql.BuildCountSql(tableName, args...)
	if err != nil {
		panic(err)
	}
	return paramArgs, whereStr
}

func (mysql *BuilderClass) BuildCountSql(tableName string, args ...interface{}) (string, []interface{}, error) {
	var whereStr = ``
	var paramArgs = []interface{}{}
	if len(args) > 0 && args[0] != nil {
		var err error
		paramArgs, whereStr, err = mysql.BuildWhere(args[0])
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

func (mysql *BuilderClass) MustBuildSumSql(tableName string, sumTarget string, args ...interface{}) (string, []interface{}) {
	str, paramArgs, err := mysql.BuildSumSql(tableName, sumTarget, args...)
	if err != nil {
		panic(err)
	}
	return str, paramArgs
}

func (mysql *BuilderClass) BuildSumSql(tableName string, sumTarget string, args ...interface{}) (string, []interface{}, error) {
	var whereStr = ``
	var paramArgs = []interface{}{}
	if len(args) > 0 && args[0] != nil {
		var err error
		paramArgs, whereStr, err = mysql.BuildWhere(args[0])
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

func (mysql *BuilderClass) buildWhereFromMapInterface(ele map[string]interface{}) ([]interface{}, string, error) {
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
			var valStr string
			if strings.HasPrefix(str, `s:`) {
				andStr = andStr + key + ` ` + str[2:] + ` and `
			} else {
				valStr = template.HTMLEscapeString(str)
				andStr = andStr + key + ` = ? and `
				tempParamArgs = append(tempParamArgs, valStr)
			}
		}
	}
	if len(andStr) > 4 {
		andStr = andStr[:len(andStr)-5]
	}
	return tempParamArgs, andStr, nil
}

func (mysql *BuilderClass) MustBuildWhere(where interface{}) ([]interface{}, string) {
	paramArgs, str, err := mysql.BuildWhere(where)
	if err != nil {
		panic(err)
	}
	return paramArgs, str
}

func (mysql *BuilderClass) BuildWhere(where interface{}) ([]interface{}, string, error) {
	type_ := reflect.TypeOf(where)
	kind := type_.Kind()
	paramArgs := []interface{}{}
	if kind == reflect.String {
		return paramArgs, where.(string), nil
	}
	whereStr := `where `
	str := ``
	if kind == reflect.Map {
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			var err error
			paramArgs, str, err = mysql.buildWhereFromMapInterface(where.(map[string]interface{}))
			if err != nil {
				return nil, ``, err
			}
		} else {
			return nil, ``, errors.New(`map value type error`)
		}
	} else if kind == reflect.Struct {
		map_, err := mysql.structToMap(where)
		if err != nil {
			return nil, ``, err
		}
		paramArgs, str, err = mysql.buildWhereFromMapInterface(map_)
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
				paramArgsTemp, str, err := mysql.buildWhereFromMapInterface(ele)
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

func (mysql *BuilderClass) MustBuildSelectSql(tableName string, select_ string, args ...interface{}) (string, []interface{}) {
	str, paramArgs, err := mysql.BuildSelectSql(tableName, select_, args...)
	if err != nil {
		panic(err)
	}
	return str, paramArgs
}

func (mysql *BuilderClass) BuildSelectSql(tableName string, select_ string, args ...interface{}) (string, []interface{}, error) {
	var whereStr = ``
	var paramArgs = []interface{}{}
	if len(args) > 0 && args[0] != nil {
		var err error
		paramArgs, whereStr, err = mysql.BuildWhere(args[0])
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

func (mysql *BuilderClass) structToMap(in_ interface{}) (map[string]interface{}, error) {
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

func (mysql *BuilderClass) MustBuildUpdateSql(tableName string, update interface{}, args ...interface{}) (string, []interface{}) {
	str, paramArgs, err := mysql.BuildUpdateSql(tableName, update, args...)
	if err != nil {
		panic(err)
	}
	return str, paramArgs
}

func (mysql *BuilderClass) BuildUpdateSql(tableName string, update interface{}, args ...interface{}) (string, []interface{}, error) {
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
		map_, err := mysql.structToMap(update)
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
		paramArgsTemp, whereStrTemp, err := mysql.BuildWhere(args[0])
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
