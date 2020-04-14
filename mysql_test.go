package go_mysql

import (
	"fmt"
	"github.com/pefish/go-logger"
	"testing"
)

func TestMysqlClass_RawExec(t *testing.T) {
	go_logger.Logger = go_logger.NewLogger(go_logger.WithIsDebug(true))
	MysqlHelper.SetLogger(go_logger.Logger)
	MysqlHelper.MustConnectWithConfiguration(Configuration{
		Host:     `127.0.0.1`,
		Username: `root`,
		Password: `root`,
		Database: `test`,
	})
	type A struct {
		Id        uint64 `json:"id"`
		Mobile    string `json:"mobile"`
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
	}
	var a []A
	MysqlHelper.MustRawSelect(&a, `select * from test where mobile in (?)`, []string{"dgh","dh"})
	fmt.Println(a)
	MysqlHelper.Close()

	//mysqlHelper1 := &MysqlClass{}
	//mysqlHelper1.Connect(`rm-bp1o91m9al1i70g7jho.mysql.rds.aliyuncs.com`, -1, `root`, `1qaz@WSX`, `test`)
	//mysqlHelper1.RawExec(`insert into user (login_pwd) values ("625ywrtywrwy")`)
	//time.Sleep(3 * time.Second)
	//mysqlHelper1.Close()
}

func TestBuilderClass_BuildUpdateSql(t *testing.T) {
	builder := BuilderClass{}
	sql, params := builder.MustBuildUpdateSql(`table`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	}, []map[string]interface{}{
		{
			`b`:    65,
			`bghf`: `352352`,
		},
		{
			`bnn`: `345`,
		},
	})
	fmt.Println(sql, params)
}

func TestBuilderClass_BuildSelectSql(t *testing.T) {
	builder := BuilderClass{}
	sql, params := builder.MustBuildSelectSql(`table`, `*`, []map[string]interface{}{
		{
			`b`:    65,
			`bghf`: `352352`,
		},
		{
			`bnn`: `345`,
		},
	})
	fmt.Println(sql, params)
}

func TestBuilderClass_BuildInsertSql(t *testing.T) {
	builder := BuilderClass{}
	sql, params := builder.MustBuildInsertSql(`table`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	}, BuildInsertSqlOpt{})
	fmt.Println(sql, params)
}

func TestBuilderClass_BuildCountSql(t *testing.T) {
	builder := BuilderClass{}
	sql, params := builder.MustBuildCountSql(`table`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	})
	fmt.Println(sql, params)
}

func TestBuilderClass_BuildSumSql(t *testing.T) {
	builder := BuilderClass{}
	sql, params := builder.MustBuildSumSql(`table`, `aa`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	})
	fmt.Println(sql, params)
}

func TestBuilderClass_BuildWhere(t *testing.T) {
	builder := BuilderClass{}
	args, sql := builder.MustBuildWhere(map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
		`b`: `s:in ("haha","hehe")`,
	})
	fmt.Println(sql, args)

	args1, sql1 := builder.MustBuildWhere(struct {
		A string  `json:"a"`
		B string  `json:"b"`
		C *string `json:"c"`
	}{
		A: `35`,
		B: `765`,
		C: nil,
	})
	fmt.Println(sql1, args1)
}

func TestMysqlClass_ConnectWithConfiguration(t *testing.T) {
	mysqlClint := MysqlClass{}
	mysqlClint.MustConnectWithConfiguration(Configuration{
		Host:     `127.0.0.1`,
		Username: `root`,
		Password: `root`,
	})
	mysqlClint.Close()
}

func TestMysqlClass_ConnectWithMap(t *testing.T) {
	mysqlClint := MysqlClass{}
	mysqlClint.MustConnectWithMap(map[string]interface{}{
		`host`:            `127.0.0.1`,
		`username`:        `root`,
		`password`:        `root`,
		`connMaxLifeTime`: 10,
	})
	mysqlClint.Close()
}
