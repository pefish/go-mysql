package p_mysql

import (
	sql2 "database/sql"
	"fmt"
	"gitee.com/pefish/p-go-application"
	"gitee.com/pefish/p-go-error"
	"gitee.com/pefish/p-go-format"
	"gitee.com/pefish/p-go-logger"
	"gitee.com/pefish/p-go-random"
	"gitee.com/pefish/p-go-reflect"
	"gitee.com/pefish/p-go-string"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"reflect"
	"strings"
	"text/template"
	"time"
)

// ----------------------------- MysqlClass -----------------------------

type MysqlClass struct {
	Db   *sqlx.DB
	TxId string
	Tx   *sqlx.Tx
}

func (this *MysqlClass) Close() {
	if this.Db != nil {
		this.Db.Close()
	}
	if this.Tx != nil {
		this.Tx.Rollback()
	}
}

func (this *MysqlClass) Connect(host string, port int64, username string, password string, database string) {
	if port == -1 {
		port = 3306
	}
	db := sqlx.MustConnect(
		`mysql`,
		fmt.Sprintf(
			`%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=true&multiStatements=true&loc=UTC`,
			username,
			password,
			host,
			port,
			database,
		),
	)
	db.DB.SetMaxOpenConns(100)                // 用于设置最大打开的连接数，默认值为0表示不限制
	db.DB.SetMaxIdleConns(30)                 // 用于设置闲置的连接数
	db.DB.SetConnMaxLifetime(6 * time.Second) // 设置一个超时时间，时间小于数据库的超时时间即可
	this.Db = db
}

func (this *MysqlClass) RawExec(sql string, values ...interface{}) (lastInsertId int64, rowsAffected int64) {
	if p_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		p_logger.Logger.Debug(fmt.Sprintf(`%s%s, %v`, txInfo, sql, values))
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
	if p_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		p_logger.Logger.Debug(fmt.Sprintf(`%s%s, %v`, txInfo, sql, values))
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
		select_ = strings.Join(p_reflect.Reflect.GetValuesInTagFromStruct(dest, `db`), `,`)
	}
	var paramArgs = []interface{}{}
	sql, paramArgs := Builder.BuildSelectSql(tableName, select_, where)
	this.RawSelect(dest, sql, paramArgs...)
}

func (this *MysqlClass) SelectFirstByMap(dest interface{}, tableName string, select_ string, where map[string]string) bool {
	if select_ == `*` {
		select_ = strings.Join(p_reflect.Reflect.GetValuesInTagFromStruct(dest, `db`), `,`)
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
		select_ = strings.Join(p_reflect.Reflect.GetValuesInTagFromStruct(dest, `db`), `,`)
	}
	sql, paramArgs := Builder.BuildSelectSql(tableName, select_, args...)
	return this.RawSelectFirst(dest, sql, paramArgs...)
}

func (this *MysqlClass) SelectById(dest interface{}, tableName string, select_ string, id string, forUpdate bool) bool {
	if select_ == `*` {
		select_ = strings.Join(p_reflect.Reflect.GetValuesInTagFromStruct(dest, `db`), `,`)
	}
	var paramArgs = []interface{}{}
	sql, paramArgs := Builder.BuildSelectSql(tableName, select_, map[string]string{
		`id`: id,
	}, nil, nil, forUpdate)
	return this.RawSelectFirst(dest, sql, paramArgs...)
}

func (this *MysqlClass) Select(dest interface{}, tableName string, select_ string, args ...interface{}) {
	if select_ == `*` {
		select_ = strings.Join(p_reflect.Reflect.GetValuesInTagFromStruct(dest, `db`), `,`)
	}
	var paramArgs = []interface{}{}
	sql, paramArgs := Builder.BuildSelectSql(tableName, select_, args...)
	this.RawSelect(dest, sql, paramArgs...)
}

func (this *MysqlClass) InsertByMap(tableName string, params map[string]string) (lastInsertId int64, rowsAffected int64) {
	sql, paramArgs := Builder.BuildInsertSql(tableName, params)
	return this.RawExec(sql, paramArgs...)
}

func (this *MysqlClass) Insert(tableName string, params interface{}) (lastInsertId int64, rowsAffected int64) {
	sql, paramArgs := Builder.BuildInsertSql(tableName, params)
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
	if p_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		p_logger.Logger.Debug(fmt.Sprintf(`%s%s, %v`, txInfo, sql, values))
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
	id := p_random.Random.GetUniqueIdString()
	if p_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		p_logger.Logger.Debug(fmt.Sprintf(`%sbegin`, txInfo))
	}

	return MysqlClass{
		Db:   nil,
		TxId: id,
		Tx:   this.Db.MustBegin(),
	}
}

func (this *MysqlClass) Commit() {
	if p_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		p_logger.Logger.Debug(fmt.Sprintf(`%scommit`, txInfo))
	}

	err := this.Tx.Commit()
	if err != nil {
		panic(err)
	}
}

func (this *MysqlClass) Rollback() {
	if p_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		p_logger.Logger.Debug(fmt.Sprintf(`%srollback`, txInfo))
	}

	err := this.Tx.Rollback()
	if err != nil {
		panic(err)
	}
}

func (this *MysqlClass) RollbackWithErr() error {
	if p_application.Application.Debug {
		txInfo := ``
		if this.Tx != nil {
			txInfo = fmt.Sprintf(`[transaction id: %s] `, this.TxId)
		}
		p_logger.Logger.Debug(fmt.Sprintf(`%srollback`, txInfo))
	}

	return this.Tx.Rollback()
}

// ----------------------------- BuilderClass -----------------------------

type BuilderClass struct {
}

var Builder = BuilderClass{}

func (this *BuilderClass) BuildInsertSql(tableName string, params interface{}) (string, []interface{}) {
	var cols []string
	var vals []string
	var paramArgs = []interface{}{}
	type_ := reflect.TypeOf(params)
	kind := type_.Kind()
	if kind == reflect.Map {
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			for key, val := range params.(map[string]interface{}) {
				cols = append(cols, key)
				vals = append(vals, `?`)
				paramArgs = append(paramArgs, template.HTMLEscapeString(p_reflect.Reflect.ToString(val)))
			}
		} else if valKind == reflect.String {
			for key, val := range params.(map[string]string) {
				cols = append(cols, key)
				vals = append(vals, `?`)
				paramArgs = append(paramArgs, template.HTMLEscapeString(val))
			}
		} else {
			p_error.ThrowInternal(`map value type error`)
		}
	} else if kind == reflect.Struct {
		for key, val := range p_format.Format.StructToMap(params) {
			if val != nil {
				if reflect.TypeOf(val).Kind() != reflect.String {
					p_error.ThrowInternal(`struct value type error`)
				}
				cols = append(cols, key)
				vals = append(vals, `?`)
				paramArgs = append(paramArgs, template.HTMLEscapeString(p_reflect.Reflect.ToString(val)))
			}
		}
	} else {
		p_error.ThrowInternal(`type error`)
	}

	str := fmt.Sprintf(
		`insert into %s (%s) values (%s)`,
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
		whereStr = this.buildWhere(&paramArgs, args[0])
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
		whereStr = this.buildWhere(&paramArgs, args[0])
	}

	str := fmt.Sprintf(
		`select sum(%s) as sum from %s %s`,
		sumTarget,
		tableName,
		whereStr,
	)
	return str, paramArgs
}

func (this *BuilderClass) buildWhereAndFromMapInterface(paramArgs *[]interface{}, ele map[string]interface{}) string {
	andStr := ``
	tempParamArgs := *paramArgs
	for key, val := range ele {
		kind := reflect.TypeOf(val).Kind()
		if kind == reflect.Slice {
			val_ := val.([]interface{})
			andStr = andStr + key + ` ` + p_reflect.Reflect.ToString(val_[0]) + ` ? and `
			tempParamArgs = append(tempParamArgs, template.HTMLEscapeString(p_reflect.Reflect.ToString(val_[1])))
		} else {
			valStr := template.HTMLEscapeString(p_reflect.Reflect.ToString(val))
			andStr = andStr + key + ` = ? and `
			tempParamArgs = append(tempParamArgs, valStr)
		}
	}
	if len(andStr) > 4 {
		andStr = p_string.String.RemoveLast(andStr, 5)
	}
	*paramArgs = tempParamArgs
	return andStr
}

func (this *BuilderClass) buildWhereAndFromMapString(paramArgs *[]interface{}, ele map[string]string) string {
	andStr := ``
	tempParamArgs := *paramArgs
	for key, val := range ele {
		valStr := template.HTMLEscapeString(val)
		andStr = andStr + key + ` = ? and `
		tempParamArgs = append(tempParamArgs, valStr)
	}
	if len(andStr) > 4 {
		andStr = p_string.String.RemoveLast(andStr, 5)
	}
	*paramArgs = tempParamArgs
	return andStr
}

func (this *BuilderClass) buildWhere(paramArgs *[]interface{}, where interface{}) string {
	whereStr := `where `
	type_ := reflect.TypeOf(where)
	kind := type_.Kind()
	if kind == reflect.String {
		return where.(string)
	}
	if kind == reflect.Map {
		addStr := ``
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			addStr = this.buildWhereAndFromMapInterface(paramArgs, where.(map[string]interface{}))
		} else if valKind == reflect.String {
			addStr = this.buildWhereAndFromMapString(paramArgs, where.(map[string]string))
		} else {
			p_error.ThrowInternal(`map value type error`)
		}
		whereStr += addStr
	} else if kind == reflect.Struct {
		for key, val := range p_format.Format.StructToMap(where) {
			if val != nil {
				if reflect.TypeOf(val).Kind() != reflect.String {
					p_error.ThrowInternal(`struct value type error`)
				}
				valStr := template.HTMLEscapeString(p_reflect.Reflect.ToString(val))
				whereStr = whereStr + key + `= "` + valStr + `" and `
			}
		}
		if len(whereStr) > 4 {
			whereStr = p_string.String.RemoveLast(whereStr, 5)
		}
	} else if kind == reflect.Slice {
		if type_.Elem().Kind() != reflect.Map {
			p_error.ThrowInternal(`slice value type error`)
		}
		mapKind := type_.Elem().Elem().Kind()
		if mapKind == reflect.Interface {
			sliceVal := where.([]map[string]interface{})
			for _, ele := range sliceVal {
				whereStr += `(` + this.buildWhereAndFromMapInterface(paramArgs, ele) + `) or `
			}
		} else {
			p_error.ThrowInternal(`map value type error`)
		}
		if len(whereStr) > 3 {
			whereStr = p_string.String.RemoveLast(whereStr, 4)
		}
	} else {
		p_error.ThrowInternal(`type error`)
	}
	return whereStr
}

func (this *BuilderClass) BuildSelectSql(tableName string, select_ string, args ...interface{}) (string, []interface{}) {
	var whereStr = ``
	var paramArgs = []interface{}{}
	if len(args) > 0 && args[0] != nil {
		whereStr = this.buildWhere(&paramArgs, args[0])
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

func (this *BuilderClass) BuildUpdateSql(tableName string, update interface{}, args ...interface{}) (string, []interface{}) {
	var updateStr = ``
	var paramArgs = []interface{}{}
	type_ := reflect.TypeOf(update)
	updateKind := type_.Kind()
	if updateKind == reflect.Map {
		valKind := type_.Elem().Kind()
		if valKind == reflect.Interface {
			for key, val := range update.(map[string]interface{}) {
				if val == `NULL` {
					updateStr = updateStr + key + ` = NULL,`
				} else {
					updateStr = updateStr + key + ` = ?,`
					paramArgs = append(paramArgs, template.HTMLEscapeString(p_reflect.Reflect.ToString(val)))
				}
			}
		} else if valKind == reflect.String {
			for key, val := range update.(map[string]string) {
				if val == `NULL` {
					updateStr = updateStr + key + ` = NULL,`
				} else {
					updateStr = updateStr + key + ` = ?,`
					paramArgs = append(paramArgs, template.HTMLEscapeString(val))
				}
			}
		} else {
			p_error.ThrowInternal(`map value type error`)
		}
	} else if updateKind == reflect.Struct {
		for key, val := range p_format.Format.StructToMap(update) {
			if val != nil {
				if reflect.TypeOf(val).Kind() != reflect.String {
					p_error.ThrowInternal(`struct value type error`)
				}
				updateStr = updateStr + key + ` = ?,`
				paramArgs = append(paramArgs, template.HTMLEscapeString(p_reflect.Reflect.ToString(val)))
			}
		}
	} else {
		p_error.ThrowInternal(`type error`)
	}
	if len(updateStr) > 0 {
		updateStr = p_string.String.RemoveLast(updateStr, 1)
	}

	var whereStr = ``
	if len(args) > 0 && args[0] != nil {
		whereStr = this.buildWhere(&paramArgs, args[0])
	}

	str := fmt.Sprintf(
		`update %s set %s %s`,
		tableName,
		updateStr,
		whereStr,
	)
	return str, paramArgs
}
