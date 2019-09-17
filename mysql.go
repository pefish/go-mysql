package go_mysql

import (
	sql2 "database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pefish/go-application"
	"github.com/pefish/go-error"
	"github.com/pefish/go-logger"
	"github.com/pefish/go-reflect"
	"github.com/satori/go.uuid"
	"reflect"
	"strings"
	"text/template"
	"time"
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

var MysqlHelper = MysqlClass{}

// ----------------------------- MysqlClass -----------------------------

type MysqlClass struct {
	Db   *sqlx.DB
	TxId string
	Tx   *sqlx.Tx
}

func (this *MysqlClass) Close() {
	if this.Db != nil {
		err := this.Db.Close()
		if err != nil {
			go_logger.Logger.Error(err)
		} else {
			go_logger.Logger.Info(`mysql close succeed.`)
		}
	}
	if this.Tx != nil {
		err := this.Tx.Rollback()
		if err != nil {
			go_logger.Logger.Error(err)
		}
	}
}

func (this *MysqlClass) ConnectWithConfiguration(configuration Configuration) {
	var port = DEFAULT_PORT
	if configuration.Port != nil {
		port = go_reflect.Reflect.ToUint64(configuration.Port)
	}
	var database *string
	if configuration.Database != nil {
		d := go_reflect.Reflect.ToString(configuration.Database)
		database = &d
	}
	var maxOpenConns = DEFAULT_MAX_OPEN_CONNS
	if configuration.MaxOpenConns != nil {
		maxOpenConns = go_reflect.Reflect.ToUint64(configuration.MaxOpenConns)
	}
	var maxIdleConns = DEFAULT_MAX_IDLE_CONNS
	if configuration.MaxIdleConns != nil {
		maxIdleConns = go_reflect.Reflect.ToUint64(configuration.MaxIdleConns)
	}
	connMaxLifetime := DEFAULT_CONN_MAX_LIFTTIME
	if configuration.ConnMaxLifetime != nil {
		connMaxLifetime = configuration.ConnMaxLifetime.(time.Duration)
	}

	this.Connect(configuration.Host, port, configuration.Username, configuration.Password, database, maxOpenConns, maxIdleConns, connMaxLifetime)
}

func (this *MysqlClass) ConnectWithMap(map_ map[string]interface{}) {
	var port = DEFAULT_PORT
	if map_[`port`] != nil {
		port = go_reflect.Reflect.ToUint64(map_[`port`])
	}
	var database *string
	if map_[`database`] != nil {
		d := go_reflect.Reflect.ToString(map_[`database`])
		database = &d
	}
	var maxOpenConns = DEFAULT_MAX_OPEN_CONNS
	if map_[`maxOpenConns`] != nil {
		maxOpenConns = go_reflect.Reflect.ToUint64(map_[`maxOpenConns`])
	}
	var maxIdleConns = DEFAULT_MAX_IDLE_CONNS
	if map_[`maxIdleConns`] != nil {
		maxIdleConns = go_reflect.Reflect.ToUint64(map_[`maxIdleConns`])
	}
	connMaxLifetime := DEFAULT_CONN_MAX_LIFTTIME
	if map_[`connMaxLifeTime`] != nil {
		fmt.Println(reflect.TypeOf(map_[`connMaxLifeTime`]).Kind())
		connMaxLifetime = time.Duration(go_reflect.Reflect.ToInt64(map_[`connMaxLifeTime`])) * time.Second
	}

	this.Connect(map_[`host`].(string), port, map_[`username`].(string), map_[`password`].(string), database, maxOpenConns, maxIdleConns, connMaxLifetime)
}

func (this *MysqlClass) Connect(host string, port uint64, username string, password string, database *string, maxOpenConns uint64, maxIdleConns uint64, connMaxLifetime time.Duration) {
	d := ``
	if database != nil {
		d = *database
	}
	address := fmt.Sprintf(`%s:%d`, host, port)
	go_logger.Logger.Info(fmt.Sprintf(`mysql connecting... url: %s`, address))
	connUrl := fmt.Sprintf(
		`%s:%s@tcp(%s)/%s?charset=utf8&parseTime=true&multiStatements=true&loc=UTC`,
		username,
		password,
		address,
		d,
	)
	db := sqlx.MustConnect(`mysql`, connUrl)
	go_logger.Logger.Info(fmt.Sprintf(`mysql connect succeed. url: %s`, address))
	db.DB.SetMaxOpenConns(int(maxOpenConns))  // 用于设置最大打开的连接数，默认值为0表示不限制
	db.DB.SetMaxIdleConns(int(maxIdleConns))  // 用于设置闲置的连接数
	db.DB.SetConnMaxLifetime(connMaxLifetime) // 设置一个超时时间，时间小于数据库的超时时间即可
	this.Db = db
}

func (this *MysqlClass) RawExec(sql string, values ...interface{}) (lastInsertId int64, rowsAffected int64) {
	if go_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		go_logger.Logger.Debug(fmt.Sprintf(`%s%s, %v`, txInfo, sql, values))
	}

	var result sql2.Result
	if this.Tx != nil {
		result = this.Tx.MustExec(sql, values...)
	} else {
		result = this.Db.MustExec(sql, values...)
	}
	lastInsertId, err := result.LastInsertId()
	if err != nil {
		panic(err)
	}
	rowsAffected, err1 := result.RowsAffected()
	if err1 != nil {
		panic(err1)
	}
	return lastInsertId, rowsAffected
}

func (this *MysqlClass) RawSelect(dest interface{}, sql string, values ...interface{}) {
	if go_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		go_logger.Logger.Debug(fmt.Sprintf(`%s%s, %v`, txInfo, sql, values))
	}

	var err error
	if this.Tx != nil {
		err = this.Tx.Select(dest, sql, values...)
	} else {
		err = this.Db.Select(dest, sql, values...)
	}
	if err != nil {
		panic(err)
	}
}

func (this *MysqlClass) CountByMap(tableName string, where map[string]string) uint64 {
	var countStruct struct {
		Count uint64 `db:"count"`
	}
	sql, paramArgs := Builder.BuildCountSql(tableName, where)
	this.RawSelectFirst(&countStruct, sql, paramArgs...)
	return countStruct.Count
}

func (this *MysqlClass) Count(tableName string, args ...interface{}) uint64 {
	var countStruct struct {
		Count uint64 `db:"count"`
	}
	sql, paramArgs := Builder.BuildCountSql(tableName, args...)
	this.RawSelectFirst(&countStruct, sql, paramArgs...)
	return countStruct.Count
}

func (this *MysqlClass) Sum(tableName string, sumTarget string, args ...interface{}) string {
	var sumStruct struct {
		Sum *string `db:"sum"`
	}
	sql, paramArgs := Builder.BuildSumSql(tableName, sumTarget, args...)
	this.RawSelectFirst(&sumStruct, sql, paramArgs...)
	if sumStruct.Sum == nil {
		return `0`
	}
	return *sumStruct.Sum
}

func (this *MysqlClass) SelectByMap(dest interface{}, tableName string, select_ string, where map[string]string) {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, `db`), `,`)
	}
	var paramArgs = []interface{}{}
	sql, paramArgs := Builder.BuildSelectSql(tableName, select_, where)
	this.RawSelect(dest, sql, paramArgs...)
}

func (this *MysqlClass) SelectFirstByMap(dest interface{}, tableName string, select_ string, where map[string]string) bool {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, `db`), `,`)
	}
	var paramArgs = []interface{}{}
	sql, paramArgs := Builder.BuildSelectSql(tableName, select_, where)
	return this.RawSelectFirst(dest, sql, paramArgs...)
}

func (this *MysqlClass) SelectColumn(columnName string, tableName string, args ...interface{}) *string {
	var resultStruct struct {
		Result string `db:"result"`
	}
	if notFound := this.SelectFirst(&resultStruct, tableName, fmt.Sprintf(`%s as result`, columnName), args...); notFound {
		return nil
	}
	return &resultStruct.Result
}

func (this *MysqlClass) SelectFirst(dest interface{}, tableName string, select_ string, args ...interface{}) bool {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, `db`), `,`)
	}
	sql, paramArgs := Builder.BuildSelectSql(tableName, select_, args...)
	return this.RawSelectFirst(dest, sql, paramArgs...)
}

func (this *MysqlClass) SelectFirstByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) bool {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, `db`), `,`)
	}
	sql := fmt.Sprintf(
		`select %s from %s %s`,
		select_,
		tableName,
		str,
	)
	return this.RawSelectFirst(dest, sql, values...)
}

func (this *MysqlClass) SelectById(dest interface{}, tableName string, select_ string, id string, forUpdate bool) bool {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, `db`), `,`)
	}
	var paramArgs = []interface{}{}
	sql, paramArgs := Builder.BuildSelectSql(tableName, select_, map[string]string{
		`id`: id,
	}, nil, nil, forUpdate)
	return this.RawSelectFirst(dest, sql, paramArgs...)
}

func (this *MysqlClass) Select(dest interface{}, tableName string, select_ string, args ...interface{}) {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, `db`), `,`)
	}
	var paramArgs = []interface{}{}
	sql, paramArgs := Builder.BuildSelectSql(tableName, select_, args...)
	this.RawSelect(dest, sql, paramArgs...)
}

func (this *MysqlClass) SelectByStr(dest interface{}, tableName string, select_ string, str string, values ...interface{}) {
	if select_ == `*` {
		select_ = strings.Join(go_reflect.Reflect.GetValuesInTagFromStruct(dest, `db`), `,`)
	}
	sql := fmt.Sprintf(
		`select %s from %s %s`,
		select_,
		tableName,
		str,
	)
	this.RawSelect(dest, sql, values...)
}

func (this *MysqlClass) InsertByMap(tableName string, params map[string]string) (lastInsertId int64, rowsAffected int64) {
	sql, paramArgs := Builder.BuildInsertSql(tableName, params, BuildInsertSqlOpt{})
	return this.RawExec(sql, paramArgs...)
}

func (this *MysqlClass) Insert(tableName string, params interface{}) (lastInsertId int64, rowsAffected int64) {
	sql, paramArgs := Builder.BuildInsertSql(tableName, params, BuildInsertSqlOpt{})
	return this.RawExec(sql, paramArgs...)
}

func (this *MysqlClass) InsertIgnore(tableName string, params interface{}) (lastInsertId int64, rowsAffected int64) {
	sql, paramArgs := Builder.BuildInsertSql(tableName, params, BuildInsertSqlOpt{
		InsertIgnore: true,
	})
	return this.RawExec(sql, paramArgs...)
}

func (this *MysqlClass) ReplaceInto(tableName string, params interface{}) (lastInsertId int64, rowsAffected int64) {
	sql, paramArgs := Builder.BuildInsertSql(tableName, params, BuildInsertSqlOpt{
		ReplaceInto: true,
	})
	return this.RawExec(sql, paramArgs...)
}

func (this *MysqlClass) UpdateByMap(tableName string, update map[string]string, where map[string]string) (lastInsertId int64, rowsAffected int64) {
	sql, paramArgs := Builder.BuildUpdateSql(tableName, update, where)
	return this.RawExec(sql, paramArgs...)
}

func (this *MysqlClass) Update(tableName string, update interface{}, args ...interface{}) (lastInsertId int64, rowsAffected int64) {
	sql, paramArgs := Builder.BuildUpdateSql(tableName, update, args...)
	return this.RawExec(sql, paramArgs...)
}

func (this *MysqlClass) RawSelectFirst(dest interface{}, sql string, values ...interface{}) (notFound bool) {
	if go_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		go_logger.Logger.Debug(fmt.Sprintf(`%s%s, %v`, txInfo, sql, values))
	}

	var err error
	if this.Tx != nil {
		err = this.Tx.Get(dest, sql, values...)
	} else {
		err = this.Db.Get(dest, sql, values...)
	}
	if err != nil && err.Error() == `sql: no rows in result set` {
		return true
	}
	if err != nil {
		panic(err)
	}

	return false
}

func (this *MysqlClass) Begin() MysqlClass {
	id := fmt.Sprintf(`%s`, uuid.NewV4())
	if go_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		go_logger.Logger.Debug(fmt.Sprintf(`%sbegin`, txInfo))
	}

	return MysqlClass{
		Db:   nil,
		TxId: id,
		Tx:   this.Db.MustBegin(),
	}
}

func (this *MysqlClass) Commit() {
	if go_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		go_logger.Logger.Debug(fmt.Sprintf(`%scommit`, txInfo))
	}

	err := this.Tx.Commit()
	if err != nil {
		panic(err)
	}
}

func (this *MysqlClass) Rollback() {
	if go_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		go_logger.Logger.Debug(fmt.Sprintf(`%srollback`, txInfo))
	}

	err := this.Tx.Rollback()
	if err != nil {
		panic(err)
	}
}

func (this *MysqlClass) RollbackWithErr() error {
	if go_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		go_logger.Logger.Debug(fmt.Sprintf(`%srollback`, txInfo))
	}

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

func (this *BuilderClass) BuildInsertSql(tableName string, params interface{}, opt BuildInsertSqlOpt) (string, []interface{}) {
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
				paramArgs = append(paramArgs, template.HTMLEscapeString(go_reflect.Reflect.ToString(val)))
			}
		} else {
			go_error.ThrowInternal(`map value type error`)
		}
	} else if kind == reflect.Struct {
		for key, val := range this.structToMap(params) {
			if val == nil {
				continue
			}
			cols = append(cols, key)
			vals = append(vals, `?`)
			paramArgs = append(paramArgs, template.HTMLEscapeString(go_reflect.Reflect.ToString(val)))
		}
	} else {
		go_error.ThrowInternal(`type error`)
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
	return str, paramArgs
}

func (this *BuilderClass) BuildCountSql(tableName string, args ...interface{}) (string, []interface{}) {
	var whereStr = ``
	var paramArgs = []interface{}{}
	if len(args) > 0 && args[0] != nil {
		paramArgs, whereStr = this.BuildWhere(args[0])
	}

	str := fmt.Sprintf(
		`select count(*) as count from %s %s`,
		tableName,
		whereStr,
	)
	return str, paramArgs
}

func (this *BuilderClass) BuildSumSql(tableName string, sumTarget string, args ...interface{}) (string, []interface{}) {
	var whereStr = ``
	var paramArgs = []interface{}{}
	if len(args) > 0 && args[0] != nil {
		paramArgs, whereStr = this.BuildWhere(args[0])
	}

	str := fmt.Sprintf(
		`select sum(%s) as sum from %s %s`,
		sumTarget,
		tableName,
		whereStr,
	)
	return str, paramArgs
}

func (this *BuilderClass) buildWhereFromMapInterface(ele map[string]interface{}) ([]interface{}, string) {
	andStr := ``
	tempParamArgs := []interface{}{}
	for key, val := range ele {
		if val == nil {
			continue
		}
		kind := reflect.TypeOf(val).Kind()
		if kind == reflect.Slice {
			val_ := val.([]interface{})
			andStr = andStr + key + ` ` + go_reflect.Reflect.ToString(val_[0]) + ` ? and `
			tempParamArgs = append(tempParamArgs, template.HTMLEscapeString(go_reflect.Reflect.ToString(val_[1])))
		} else {
			valStr := template.HTMLEscapeString(go_reflect.Reflect.ToString(val))
			andStr = andStr + key + ` = ? and `
			tempParamArgs = append(tempParamArgs, valStr)
		}
	}
	if len(andStr) > 4 {
		andStr = andStr[:len(andStr)-5]
	}
	return tempParamArgs, andStr
}

func (this *BuilderClass) BuildWhere(where interface{}) ([]interface{}, string) {
	whereStr := `where `
	type_ := reflect.TypeOf(where)
	kind := type_.Kind()
	paramArgs := []interface{}{}
	if kind == reflect.String {
		return paramArgs, where.(string)
	}
	str := ``
	if kind == reflect.Map {
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			paramArgs, str = this.buildWhereFromMapInterface(where.(map[string]interface{}))
		} else {
			go_error.ThrowInternal(`map value type error`)
		}
	} else if kind == reflect.Struct {
		paramArgs, str = this.buildWhereFromMapInterface(this.structToMap(where))
	} else if kind == reflect.Slice { // or
		if type_.Elem().Kind() != reflect.Map {
			go_error.ThrowInternal(`slice value type error`)
		}
		mapKind := type_.Elem().Elem().Kind()
		if mapKind == reflect.Interface {
			sliceVal := where.([]map[string]interface{})
			for _, ele := range sliceVal {
				paramArgsTemp, str := this.buildWhereFromMapInterface(ele)
				paramArgs = append(paramArgs, paramArgsTemp...)
				whereStr += `(` + str + `) or `
			}
		} else {
			go_error.ThrowInternal(`map value type error`)
		}
		if len(whereStr) > 3 {
			whereStr = whereStr[:len(whereStr)-4]
		}
	} else {
		go_error.ThrowInternal(`where type error`)
	}
	return paramArgs, whereStr + str
}

func (this *BuilderClass) BuildSelectSql(tableName string, select_ string, args ...interface{}) (string, []interface{}) {
	var whereStr = ``
	var paramArgs = []interface{}{}
	if len(args) > 0 && args[0] != nil {
		paramArgs, whereStr = this.BuildWhere(args[0])
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
	return str, paramArgs
}

func (this *BuilderClass) structToMap(in_ interface{}) map[string]interface{} {
	var result map[string]interface{}
	inrec, err := json.Marshal(in_)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(inrec, &result)
	if err != nil {
		panic(err)
	}
	return result
}

func (this *BuilderClass) BuildUpdateSql(tableName string, update interface{}, args ...interface{}) (string, []interface{}) {
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
				paramArgs = append(paramArgs, template.HTMLEscapeString(go_reflect.Reflect.ToString(val)))
			}
		} else {
			go_error.ThrowInternal(`map value type error`)
		}
	} else if updateKind == reflect.Struct {
		for key, val := range this.structToMap(update) {
			if val == nil {
				continue
			}
			updateStr = updateStr + key + ` = ?,`
			paramArgs = append(paramArgs, template.HTMLEscapeString(go_reflect.Reflect.ToString(val)))
		}
	} else {
		go_error.ThrowInternal(`type error`)
	}
	if len(updateStr) > 0 {
		updateStr = updateStr[:len(updateStr)-1]
	}

	var whereStr = ``
	if len(args) > 0 && args[0] != nil {
		paramArgsTemp, whereStrTemp := this.BuildWhere(args[0])
		paramArgs = append(paramArgs, paramArgsTemp...)
		whereStr = whereStrTemp
	}

	str := fmt.Sprintf(
		`update %s set %s %s`,
		tableName,
		updateStr,
		whereStr,
	)
	return str, paramArgs
}
