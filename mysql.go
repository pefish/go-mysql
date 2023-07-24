package go_mysql

import (
	sql2 "database/sql"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"reflect"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pefish/go-logger"
	"github.com/pefish/go-mysql/sqlx"
	go_reflect "github.com/pefish/go-reflect"
	uuid "github.com/satori/go.uuid"
)

type IMysql interface {
	TagName() string
	SetLogger(logger go_logger.InterfaceLogger)
	Close()

	MustConnectWithConfiguration(configuration Configuration)
	ConnectWithConfiguration(configuration Configuration) error
	MustConnectWithMap(map_ map[string]interface{})
	ConnectWithMap(map_ map[string]interface{}) error
	Connect(host string, port uint64, username string, password string, database *string, maxOpenConns uint64, maxIdleConns uint64, connMaxLifetime time.Duration) error

	MustRawSelectByStr(dest interface{}, select_ string, str string, values ...interface{})
	MustRawExec(sql string, values ...interface{}) (uint64, uint64)
	RawSelectByStr(dest interface{}, select_ string, str string, values ...interface{}) error
	RawExec(sql string, values ...interface{}) (uint64, uint64, error)
	MustRawSelect(dest interface{}, sql string, values ...interface{})
	RawSelect(dest interface{}, sql string, values ...interface{}) error
	MustCount(tableName string, args ...interface{}) uint64
	Count(tableName string, args ...interface{}) (uint64, error)
	RawCount(tableName string, args ...interface{}) (uint64, error)
	MustSum(tableName string, sumTarget string, args ...interface{}) string
	Sum(tableName string, sumTarget string, args ...interface{}) (string, error)
	MustSelectFirst(dest interface{}, tableName string, select_ string, args ...interface{}) bool
	SelectFirst(dest interface{}, tableName string, select_ string, args ...interface{}) (bool, error)
	SelectFieldStrFirst(fieldName string, tableName string, args ...interface{}) (bool, *string, error)
	MustSelectFirstByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) bool
	SelectFirstByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) (bool, error)
	MustSelectById(dest interface{}, tableName string, select_ string, id uint64, forUpdate bool) bool
	SelectById(dest interface{}, tableName string, select_ string, id uint64, forUpdate bool) (notFound bool, err error)
	MustSelect(dest interface{}, tableName string, select_ string, args ...interface{})
	Select(dest interface{}, tableName string, select_ string, args ...interface{}) error
	MustSelectByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{})
	SelectByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) error
	MustAffectedInsert(tableName string, params interface{}) (lastInsertId uint64)
	MustInsert(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64)
	Insert(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error)
	MustInsertIgnore(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64)
	InsertIgnore(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error)
	InsertOnDuplicateKeyUpdate(tableName string, update map[string]interface{}, otherParams map[string]interface{}) (lastInsertId uint64, rowsAffected uint64, err error)
	MustReplaceInto(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64)
	ReplaceInto(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error)
	MustUpdate(tableName string, update interface{}, args ...interface{}) (lastInsertId uint64, rowsAffected uint64)
	Update(tableName string, update interface{}, args ...interface{}) (lastInsertId uint64, rowsAffected uint64, err error)
	MustAffectedUpdate(tableName string, update interface{}, args ...interface{}) (lastInsertId uint64)
	MustRawSelectFirst(dest interface{}, sql string, values ...interface{}) bool
	RawSelectFirst(dest interface{}, sql string, values ...interface{}) (bool, error)

	MustBegin() *MysqlClass
	Begin() (*MysqlClass, error)
	MustCommit()
	Commit() error
	MustRollback()
	Rollback() error
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

var MysqlInstance IMysql = &MysqlClass{
	tagName: `json`,
	logger:  go_logger.DefaultLogger,
}

// ----------------------------- MysqlClass -----------------------------

type MysqlClass struct {
	Db      *sqlx.DB
	TxId    string
	Tx      *sqlx.Tx
	tagName string
	logger  go_logger.InterfaceLogger
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
			mc.logger.Info(`mc close succeed.`)
		}
	}
	if mc.Tx != nil {
		err := mc.Tx.Rollback()
		if err != nil {
			mc.logger.Error(err)
		}
	}
}

func (mc *MysqlClass) MustConnectWithConfiguration(configuration Configuration) {
	err := mc.ConnectWithConfiguration(configuration)
	if err != nil {
		panic(err)
	}
}

func (mc *MysqlClass) ConnectWithConfiguration(configuration Configuration) error {
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
		d := go_reflect.Reflect.ToString(configuration.Database)
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

	err := mc.Connect(configuration.Host, port, configuration.Username, configuration.Password, database, maxOpenConns, maxIdleConns, connMaxLifetime)
	if err != nil {
		return err
	}
	return nil
}

func (mc *MysqlClass) MustConnectWithMap(map_ map[string]interface{}) {
	err := mc.ConnectWithMap(map_)
	if err != nil {
		panic(err)
	}
}

func (mc *MysqlClass) ConnectWithMap(map_ map[string]interface{}) error {
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
		d := go_reflect.Reflect.ToString(map_[`database`])
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

	err := mc.Connect(map_[`host`].(string), port, map_[`username`].(string), map_[`password`].(string), database, maxOpenConns, maxIdleConns, connMaxLifetime)
	if err != nil {
		return err
	}
	return nil
}

func (mc *MysqlClass) Connect(host string, port uint64, username string, password string, database *string, maxOpenConns uint64, maxIdleConns uint64, connMaxLifetime time.Duration) error {
	d := ``
	if database != nil {
		d = *database
	}
	address := fmt.Sprintf(`%s:%d`, host, port)
	mc.logger.Info(fmt.Sprintf(`mc connecting... url: %s`, address))
	connUrl := fmt.Sprintf(
		`%s:%s@tcp(%s)/%s?charset=utf8&parseTime=true&multiStatements=true&loc=UTC`,
		username,
		password,
		address,
		d,
	)
	db, err := sqlx.Connect(`mc`, connUrl)
	if err != nil {
		return err
	}
	db.SetTagName(mc.tagName)
	mc.logger.Info(fmt.Sprintf(`mc connect succeed. url: %s`, address))
	db.DB.SetMaxOpenConns(int(maxOpenConns))  // 用于设置最大打开的连接数，默认值为0表示不限制
	db.DB.SetMaxIdleConns(int(maxIdleConns))  // 用于设置闲置的连接数
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

func (mc *MysqlClass) MustRawSelectByStr(dest interface{}, select_ string, str string, values ...interface{}) {
	err := mc.RawSelectByStr(dest, select_, str, values...)
	if err != nil {
		panic(err)
	}
}

func (mc *MysqlClass) RawSelectByStr(dest interface{}, select_ string, str string, values ...interface{}) error {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mc.tagName), `,`)
	}
	sql := fmt.Sprintf(
		`select %s %s`,
		select_,
		str,
	)
	err := mc.RawSelect(dest, sql, values...)
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
			return ``, nil, err
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

func (mc *MysqlClass) MustRawSelect(dest interface{}, sql string, values ...interface{}) {
	err := mc.RawSelect(dest, sql, values...)
	if err != nil {
		panic(err)
	}
}

func (mc *MysqlClass) correctSelectStar(dest interface{}, sql string) string {
	sql = strings.TrimLeft(sql, " \n\t")
	if strings.HasPrefix(sql, `select *`) {
		return `select ` + strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mc.tagName), `,`) + sql[8:]
	}

	return sql
}

func (mc *MysqlClass) RawSelect(dest interface{}, sql string, values ...interface{}) error {
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
		return err
	}
	return nil
}

func (mc *MysqlClass) MustCount(tableName string, args ...interface{}) uint64 {
	result, err := mc.Count(tableName, args...)
	if err != nil {
		panic(err)
	}
	return result
}

func (mc *MysqlClass) Count(tableName string, args ...interface{}) (uint64, error) {
	var countStruct struct {
		Count uint64 `json:"count"`
	}
	sql, paramArgs, err := builder.BuildCountSql(tableName, args...)
	if err != nil {
		return 0, err
	}
	_, err = mc.RawSelectFirst(&countStruct, sql, paramArgs...)
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
		return 0, err
	}
	return countStruct.Count, nil
}

func (mc *MysqlClass) MustSum(tableName string, sumTarget string, args ...interface{}) string {
	result, err := mc.Sum(tableName, sumTarget, args...)
	if err != nil {
		panic(err)
	}
	return result
}

func (mc *MysqlClass) Sum(tableName string, sumTarget string, args ...interface{}) (string, error) {
	var sumStruct struct {
		Sum *string `json:"sum"`
	}
	sql, paramArgs, err := builder.BuildSumSql(tableName, sumTarget, args...)
	if err != nil {
		return ``, err
	}
	_, err = mc.RawSelectFirst(&sumStruct, sql, paramArgs...)
	if err != nil {
		return ``, err
	}
	if sumStruct.Sum == nil {
		return `0`, nil
	}
	return *sumStruct.Sum, nil
}

func (mc *MysqlClass) MustSelectFirst(dest interface{}, tableName string, select_ string, args ...interface{}) bool {
	bool_, err := mc.SelectFirst(dest, tableName, select_, args...)
	if err != nil {
		panic(err)
	}
	return bool_
}

func (mc *MysqlClass) SelectFirst(dest interface{}, tableName string, select_ string, args ...interface{}) (bool, error) {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mc.tagName), `,`)
	}
	sql, paramArgs, err := builder.BuildSelectSql(tableName, select_, args...)
	if err != nil {
		return true, err
	}
	return mc.RawSelectFirst(dest, sql, paramArgs...)
}

func (mc *MysqlClass) SelectFieldStrFirst(fieldName string, tableName string, args ...interface{}) (bool, *string, error) {
	sql, paramArgs, err := builder.BuildSelectSql(tableName, fmt.Sprintf("%s as target", fieldName), args...)
	if err != nil {
		return true, nil, err
	}
	var targetStruct struct {
		Target *string `json:"target"`
	}
	notFound, err := mc.RawSelectFirst(&targetStruct, sql, paramArgs...)
	if err != nil || notFound {
		return notFound, nil, err
	}
	return false, targetStruct.Target, nil
}

func (mc *MysqlClass) MustSelectFirstByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) bool {
	bool_, err := mc.SelectFirstByStr(dest, tableName, select_, str, values...)
	if err != nil {
		panic(err)
	}
	return bool_
}

func (mc *MysqlClass) SelectFirstByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) (bool, error) {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mc.tagName), `,`)
	}
	sql := fmt.Sprintf(
		`select %s from %s %s`,
		select_,
		tableName,
		str,
	)
	return mc.RawSelectFirst(dest, sql, values...)
}

func (mc *MysqlClass) MustSelectById(dest interface{}, tableName string, select_ string, id uint64, forUpdate bool) bool {
	bool_, err := mc.SelectById(dest, tableName, select_, id, forUpdate)
	if err != nil {
		panic(err)
	}
	return bool_
}

func (mc *MysqlClass) SelectById(dest interface{}, tableName string, select_ string, id uint64, forUpdate bool) (notFound bool, err error) {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mc.tagName), `,`)
	}
	var paramArgs = []interface{}{}
	sql, paramArgs, err := builder.BuildSelectSql(tableName, select_, map[string]interface{}{
		`id`: id,
	}, nil, nil, forUpdate)
	if err != nil {
		return true, err
	}
	return mc.RawSelectFirst(dest, sql, paramArgs...)
}

func (mc *MysqlClass) MustSelect(dest interface{}, tableName string, select_ string, args ...interface{}) {
	err := mc.Select(dest, tableName, select_, args...)
	if err != nil {
		panic(err)
	}
}

func (mc *MysqlClass) Select(dest interface{}, tableName string, select_ string, args ...interface{}) error {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mc.tagName), `,`)
	}
	var paramArgs = []interface{}{}
	sql, paramArgs, err := builder.BuildSelectSql(tableName, select_, args...)
	if err != nil {
		return err
	}
	err = mc.RawSelect(dest, sql, paramArgs...)
	if err != nil {
		return err
	}
	return nil
}

func (mc *MysqlClass) MustSelectByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) {
	err := mc.SelectByStr(dest, tableName, select_, str, values...)
	if err != nil {
		panic(err)
	}
}

func (mc *MysqlClass) SelectByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) error {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, mc.tagName), `,`)
	}
	sql := fmt.Sprintf(
		`select %s from %s %s`,
		select_,
		tableName,
		str,
	)
	err := mc.RawSelect(dest, sql, values...)
	if err != nil {
		return err
	}
	return nil
}

func (mc *MysqlClass) MustAffectedInsert(tableName string, params interface{}) (lastInsertId uint64) {
	lastInsertId, rowsAffected, err := mc.Insert(tableName, params)
	if err != nil {
		panic(err)
	}
	if rowsAffected == 0 {
		panic(errors.New(`no affected`))
	}
	return lastInsertId
}

func (mc *MysqlClass) MustInsert(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64) {
	lastInsertId, rowsAffected, err := mc.Insert(tableName, params)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (mc *MysqlClass) Insert(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := builder.BuildInsertSql(tableName, params, buildInsertSqlOpt{})
	if err != nil {
		return 0, 0, err
	}
	return mc.RawExec(sql, paramArgs...)
}

func (mc *MysqlClass) MustInsertIgnore(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64) {
	lastInsertId, rowsAffected, err := mc.InsertIgnore(tableName, params)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (mc *MysqlClass) InsertIgnore(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := builder.BuildInsertSql(tableName, params, buildInsertSqlOpt{
		InsertIgnore: true,
	})
	if err != nil {
		return 0, 0, err
	}
	return mc.RawExec(sql, paramArgs...)
}

func (mc *MysqlClass) InsertOnDuplicateKeyUpdate(tableName string, update map[string]interface{}, otherParams map[string]interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	for k, v := range update {
		otherParams[k] = v
	}
	sql, paramArgs, err := builder.BuildInsertSql(tableName, otherParams, buildInsertSqlOpt{
		OnDuplicateKeyUpdate: update,
	})
	if err != nil {
		return 0, 0, err
	}
	return mc.RawExec(sql, paramArgs...)
}

func (mc *MysqlClass) MustReplaceInto(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64) {
	lastInsertId, rowsAffected, err := mc.ReplaceInto(tableName, params)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (mc *MysqlClass) ReplaceInto(tableName string, params interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := builder.BuildInsertSql(tableName, params, buildInsertSqlOpt{
		ReplaceInto: true,
	})
	if err != nil {
		return 0, 0, err
	}
	return mc.RawExec(sql, paramArgs...)
}

func (mc *MysqlClass) MustUpdate(tableName string, update interface{}, args ...interface{}) (lastInsertId uint64, rowsAffected uint64) {
	lastInsertId, rowsAffected, err := mc.Update(tableName, update, args...)
	if err != nil {
		panic(err)
	}
	return lastInsertId, rowsAffected
}

func (mc *MysqlClass) Update(tableName string, update interface{}, args ...interface{}) (lastInsertId uint64, rowsAffected uint64, err error) {
	sql, paramArgs, err := builder.BuildUpdateSql(tableName, update, args...)
	if err != nil {
		return 0, 0, err
	}
	return mc.RawExec(sql, paramArgs...)
}

func (mc *MysqlClass) MustAffectedUpdate(tableName string, update interface{}, args ...interface{}) (lastInsertId uint64) {
	lastInsertId, rowsAffected, err := mc.Update(tableName, update, args...)
	if err != nil {
		panic(err)
	}
	if rowsAffected == 0 {
		panic(errors.New(`no affected`))
	}
	return lastInsertId
}

func (mc *MysqlClass) MustRawSelectFirst(dest interface{}, sql string, values ...interface{}) bool {
	notFound, err := mc.RawSelectFirst(dest, sql, values...)
	if err != nil {
		panic(err)
	}
	return notFound
}

func (mc *MysqlClass) RawSelectFirst(dest interface{}, sql string, values ...interface{}) (bool, error) {
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
			return true, err
		}
	}

	return false, nil
}

func (mc *MysqlClass) MustBegin() *MysqlClass {
	c, err := mc.Begin()
	if err != nil {
		panic(err)
	}
	return c
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

func (mc *MysqlClass) MustCommit() {
	err := mc.Commit()
	if err != nil {
		panic(err)
	}
}

func (mc *MysqlClass) Commit() error {
	mc.printDebugInfo(`commit`, nil)

	err := mc.Tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (mc *MysqlClass) MustRollback() {
	err := mc.Rollback()
	if err != nil {
		panic(err)
	}
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

type buildInsertSqlOpt struct {
	InsertIgnore         bool
	ReplaceInto          bool
	OnDuplicateKeyUpdate map[string]interface{}
}

func (mysql *builderClass) MustBuildInsertSql(tableName string, params interface{}, opt buildInsertSqlOpt) (string, []interface{}) {
	str, paramArgs, err := mysql.BuildInsertSql(tableName, params, opt)
	if err != nil {
		panic(err)
	}
	return str, paramArgs
}

func (mysql *builderClass) BuildInsertSql(tableName string, params interface{}, opt buildInsertSqlOpt) (string, []interface{}, error) {
	var cols []string
	var vals []string
	var paramArgs = make([]interface{}, 0)
	type_ := reflect.TypeOf(params)
	kind := type_.Kind()
	if kind == reflect.Map {
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			cols, _, vals, paramArgs = mysql.buildFromMap(params.(map[string]interface{}))
		} else {
			return ``, nil, errors.New(`map value type error`)
		}
	} else if kind == reflect.Struct {
		map_, err := mysql.structToMap(params)
		if err != nil {
			return ``, nil, err
		}
		cols, _, vals, paramArgs = mysql.buildFromMap(map_)
	} else if kind == reflect.Slice {
		value_ := reflect.ValueOf(params)
		if value_.Len() == 0 {
			return "", nil, errors.New("slice length cannot be 0")
		}
		map_, err := mysql.structToMap(value_.Index(0).Interface())
		if err != nil {
			return ``, nil, err
		}
		for key, _ := range map_ {
			cols = append(cols, key)
		}
		q := squirrel.Insert(tableName).Columns(cols...)
		for i := 0; i < value_.Len(); i++ {

			map_, err := mysql.structToMap(value_.Index(i).Interface())
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
	} else {
		return ``, nil, errors.New(`type error`)
	}

	insertStr := `insert`
	if opt.InsertIgnore == true {
		insertStr += ` ignore`
	} else if opt.ReplaceInto == true {
		insertStr = ` replace into`
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
			paramArgs = append(paramArgs, go_reflect.Reflect.ToString(val))
		}
		str = strings.TrimSuffix(str, ",")
	}
	return str, paramArgs, nil
}

func (mysql *builderClass) MustBuildCountSql(tableName string, args ...interface{}) (string, []interface{}) {
	paramArgs, whereStr, err := mysql.BuildCountSql(tableName, args...)
	if err != nil {
		panic(err)
	}
	return paramArgs, whereStr
}

func (mysql *builderClass) BuildCountSql(tableName string, args ...interface{}) (string, []interface{}, error) {
	var whereStr = ``
	var paramArgs = []interface{}{}
	if len(args) > 0 && args[0] != nil {
		var err error
		paramArgs, whereStr, err = mysql.BuildWhere(args[0], args[1:])
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

func (mysql *builderClass) MustBuildSumSql(tableName string, sumTarget string, args ...interface{}) (string, []interface{}) {
	str, paramArgs, err := mysql.BuildSumSql(tableName, sumTarget, args...)
	if err != nil {
		panic(err)
	}
	return str, paramArgs
}

func (mysql *builderClass) BuildSumSql(tableName string, sumTarget string, args ...interface{}) (string, []interface{}, error) {
	var whereStr = ``
	var paramArgs = []interface{}{}
	if len(args) > 0 && args[0] != nil {
		var err error
		paramArgs, whereStr, err = mysql.BuildWhere(args[0], args[1:])
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

func (mysql *builderClass) buildWhereFromMap(ele map[string]interface{}) ([]interface{}, string, error) {
	cols, ops, vals, args := mysql.buildFromMap(ele)

	andStr := ``
	for i, col := range cols {
		andStr = andStr + col + " " + ops[i] + " " + vals[i] + ` and `
	}
	if len(andStr) > 4 {
		andStr = andStr[:len(andStr)-5]
	}
	return args, andStr, nil
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
		cols = append(cols, key)

		str := go_reflect.Reflect.ToString(val)
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
	return
}

func (mysql *builderClass) MustBuildWhere(where interface{}, args []interface{}) ([]interface{}, string) {
	paramArgs, str, err := mysql.BuildWhere(where, args)
	if err != nil {
		panic(err)
	}
	return paramArgs, str
}

func (mysql *builderClass) BuildWhere(where interface{}, args []interface{}) ([]interface{}, string, error) {
	type_ := reflect.TypeOf(where)
	kind := type_.Kind()
	paramArgs := args
	whereStr := `where `
	str := ``
	if kind == reflect.Map {
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			var err error
			paramArgs, str, err = mysql.buildWhereFromMap(where.(map[string]interface{}))
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
		paramArgs, str, err = mysql.buildWhereFromMap(map_)
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
				paramArgsTemp, str, err := mysql.buildWhereFromMap(ele)
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
	} else if kind == reflect.String {
		return paramArgs, where.(string), nil
	} else {
		return nil, ``, errors.New(`where type error`)
	}
	return paramArgs, whereStr + str, nil
}

func (mysql *builderClass) MustBuildSelectSql(tableName string, select_ string, args ...interface{}) (string, []interface{}) {
	str, paramArgs, err := mysql.BuildSelectSql(tableName, select_, args...)
	if err != nil {
		panic(err)
	}
	return str, paramArgs
}

func (mysql *builderClass) BuildSelectSql(tableName string, select_ string, args ...interface{}) (string, []interface{}, error) {
	var whereStr = ``
	var paramArgs = []interface{}{}
	if len(args) > 0 && args[0] != nil {
		var err error
		paramArgs, whereStr, err = mysql.BuildWhere(args[0], args[1:])
		if err != nil {
			return ``, nil, err
		}
	}

	str := fmt.Sprintf(
		`select %s from %s %s`,
		select_,
		tableName,
		whereStr,
	)
	return str, paramArgs, nil
}

func (mysql *builderClass) structToMap(in_ interface{}) (map[string]interface{}, error) {
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

func (mysql *builderClass) MustBuildUpdateSql(tableName string, update interface{}, args ...interface{}) (string, []interface{}) {
	str, paramArgs, err := mysql.BuildUpdateSql(tableName, update, args...)
	if err != nil {
		panic(err)
	}
	return str, paramArgs
}

func (mysql *builderClass) BuildUpdateSql(tableName string, update interface{}, args ...interface{}) (string, []interface{}, error) {
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
				paramArgs = append(paramArgs, go_reflect.Reflect.ToString(val))
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
			paramArgs = append(paramArgs, go_reflect.Reflect.ToString(val))
		}
	} else {
		return ``, nil, errors.New(`type error`)
	}
	if len(updateStr) > 0 {
		updateStr = updateStr[:len(updateStr)-1]
	}

	var whereStr = ``
	if len(args) > 0 && args[0] != nil {
		paramArgsTemp, whereStrTemp, err := mysql.BuildWhere(args[0], args[1:])
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
