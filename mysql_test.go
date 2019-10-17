package go_mysql

import (
	"fmt"
	"testing"
)

func TestMysqlClass_RawExec(t *testing.T) {
	mysqlHelper := &MysqlClass{}
	mysqlHelper.ConnectWithConfiguration(Configuration{
		Host:     `127.0.0.1`,
		Username: `root`,
		Password: `root`,
		Database: `test`,
	})
	type A struct {
		Id     uint64 `db:"id"`
		Mobile string `db:"mobile"`
		CreatedAt string `db:"created_at" json:"created_at"`
		UpdatedAt string `db:"updated_at" json:"updated_at"`
	}
	var a []A
	mysqlHelper.RawSelect(&a, `select * from test where mobile = ?`, 11)
	fmt.Println(a)
	mysqlHelper.Close()

	//mysqlHelper1 := &MysqlClass{}
	//mysqlHelper1.Connect(`rm-bp1o91m9al1i70g7jho.mysql.rds.aliyuncs.com`, -1, `root`, `1qaz@WSX`, `test`)
	//mysqlHelper1.RawExec(`insert into user (login_pwd) values ("625ywrtywrwy")`)
	//time.Sleep(3 * time.Second)
	//mysqlHelper1.Close()
}

func TestBuilderClass_BuildUpdateSql(t *testing.T) {
	builder := BuilderClass{}
	sql, params := builder.BuildUpdateSql(`table`, map[string]interface{}{
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
	sql, params := builder.BuildSelectSql(`table`, `*`, []map[string]interface{}{
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
	sql, params := builder.BuildInsertSql(`table`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	}, BuildInsertSqlOpt{})
	fmt.Println(sql, params)
}

func TestBuilderClass_BuildCountSql(t *testing.T) {
	builder := BuilderClass{}
	sql, params := builder.BuildCountSql(`table`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	})
	fmt.Println(sql, params)
}

func TestBuilderClass_BuildSumSql(t *testing.T) {
	builder := BuilderClass{}
	sql, params := builder.BuildSumSql(`table`, `aa`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	})
	fmt.Println(sql, params)
}

func TestBuilderClass_BuildWhere(t *testing.T) {
	builder := BuilderClass{}
	args, sql := builder.BuildWhere(map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
		`b`: nil,
	})
	fmt.Println(sql, args)

	args1, sql1 := builder.BuildWhere(struct {
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
	mysqlClint.ConnectWithConfiguration(Configuration{
		Host:     `127.0.0.1`,
		Username: `root`,
		Password: `root`,
	})
	mysqlClint.Close()
}

func TestMysqlClass_ConnectWithMap(t *testing.T) {
	mysqlClint := MysqlClass{}
	mysqlClint.ConnectWithMap(map[string]interface{}{
		`host`:            `127.0.0.1`,
		`username`:        `root`,
		`password`:        `root`,
		`connMaxLifeTime`: 10,
	})
	mysqlClint.Close()
}
