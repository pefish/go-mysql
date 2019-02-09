package p_mysql

import (
	"fmt"
	"testing"
)

func TestMysqlClass_RawExec(t *testing.T) {
	//mysqlHelper := &MysqlClass{}
	//mysqlHelper.Connect(`127.0.0.1`, -1, `root`, `root`, `test`)
	//mysqlHelper.RawExec(`insert into user (login_pwd) values ("625ywrtywrwy")`)
	//mysqlHelper.Close()

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
			`b`: 65,
			`bghf`: `352352`,
		},
		{
			`bnn`: `345`,
		},
	})
	fmt.Println(sql, params)
}
