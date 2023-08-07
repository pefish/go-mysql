package go_mysql

import (
	"fmt"
	go_logger "github.com/pefish/go-logger"
	"github.com/pefish/go-test-assert"
	"strings"
	"testing"
)

func TestBuilderClass_BuildUpdateSql(t *testing.T) {
	builder := builderClass{}
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
	test.Equal(t, true, strings.HasPrefix(sql, "update table set "))
	test.Equal(t, 5, len(params))

	sql1, params1 := builder.MustBuildUpdateSql(`table`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	}, `where b = ? and f = ?`, 23, 19)
	test.Equal(t, "update table set a = ?,c = ? where b = ? and f = ?", sql1)
	test.Equal(t, 4, len(params1))
}

func TestBuilderClass_BuildInsertSql(t *testing.T) {
	builder := builderClass{}
	sql, params := builder.MustBuildInsertSql(`table`, map[string]interface{}{
		`a`: 123,
		`c`: `s: = hfhd`,
	}, buildInsertSqlOpt{})
	test.Equal(t, true, strings.HasPrefix(sql, "insert into table "))
	test.Equal(t, 1, len(params))

	sql1, params1 := builder.MustBuildInsertSql(`table`, []map[string]interface{}{
		{
			`a`: 123,
			`c`: `hfhd`,
			`b`: 431,
		},
		{
			`a`: 345,
			`c`: `aaa`,
			`b`: 524,
		},
		{
			`a`: 444,
			`c`: `qqq`,
			`b`: 111,
		},
	}, buildInsertSqlOpt{})
	//fmt.Println(sql1, params1)
	test.Equal(t, true, strings.HasPrefix(sql1, "INSERT INTO table "))
	test.Equal(t, 9, len(params1))

	type Test struct {
		A string `json:"a"`
		B uint64 `json:"b"`
	}
	sql2, params2 := builder.MustBuildInsertSql(`table`, []Test{
		{
			B: 123,
			A: `hfhd`,
		},
		{
			B: 345,
			A: `aaa`,
		},
	}, buildInsertSqlOpt{})
	//fmt.Println(sql2, params2)
	test.Equal(t, true, strings.HasPrefix(sql2, "INSERT INTO table "))
	test.Equal(t, 4, len(params2))
}

func TestBuilderClass_BuildCountSql(t *testing.T) {
	builder := builderClass{}
	sql, params := builder.MustBuildCountSql(`table`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	})
	test.Equal(t, true, strings.HasPrefix(sql, "select count(*) as count from table where "))
	test.Equal(t, 2, len(params))
}

func TestBuilderClass_BuildSumSql(t *testing.T) {
	builder := builderClass{}
	sql, params := builder.MustBuildSumSql(`table`, `aa`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	})
	test.Equal(t, true, strings.HasPrefix(sql, "select sum(aa) as sum from table where "))
	test.Equal(t, 2, len(params))
}

func TestBuilderClass_BuildWhere(t *testing.T) {
	builder := builderClass{}
	args, sql := builder.MustBuildWhere(map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
		`b`: `s:in ("haha","hehe")`,
	}, nil)
	test.Equal(t, true, strings.HasPrefix(sql, "where "))
	test.Equal(t, 2, len(args))

	args1, sql1 := builder.MustBuildWhere(struct {
		A string  `json:"a"`
		B string  `json:"b"`
		C *string `json:"c"`
	}{
		A: `35`,
		B: `765`,
		C: nil,
	}, nil)
	test.Equal(t, true, strings.HasPrefix(sql1, "where "))
	test.Equal(t, 2, len(args1))

	args2, sql2 := builder.MustBuildWhere(`where a = ? and b = ?`, []interface{}{"1", "2"})
	test.Equal(t, "where a = ? and b = ?", sql2)
	test.Equal(t, 2, len(args2))

}

func TestBuilderClass_BuildInsertSql1(t *testing.T) {
	builder := builderClass{}
	sql, params := builder.MustBuildInsertSql(`table`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	}, buildInsertSqlOpt{
		OnDuplicateKeyUpdate: map[string]interface{}{
			"a": 235,
		},
	})
	test.Equal(t, true, strings.HasPrefix(sql, "insert into table "))
	test.Equal(t, 3, len(params))
}

func Test_builderClass_buildWhereFromMapInterface(t *testing.T) {
	builder := builderClass{}
	params, sql, err := builder.buildWhereFromMap(map[string]interface{}{
		`a`: 123,
		`c`: "s: in (35)",
	})
	test.Equal(t, nil, err)
	test.Equal(t, "a = ? and c in (35)", sql)
	test.Equal(t, 1, len(params))
}

func Test_builderClass_buildWhereFromMapInterface1(t *testing.T) {
	builder := builderClass{}
	params, sql, err := builder.buildWhereFromMap(map[string]interface{}{
		`a`: 123,
		`c`: []interface{}{
			35,
			27,
			11,
		},
	})
	test.Equal(t, nil, err)
	test.In(t, []interface{}{
		"a = ? and c in (?,?,?)",
		"c in (?,?,?) and a = ?",
	}, sql)
	test.Equal(t, 4, len(params))

	params1, sql1, err1 := builder.buildWhereFromMap(map[string]interface{}{
		`a`: 123,
		`c`: []interface{}{},
	})
	test.Equal(t, nil, err1)
	test.Equal(t, "a = ?", sql1)
	test.Equal(t, 1, len(params1))

	params2, sql2, err2 := builder.buildWhereFromMap(map[string]interface{}{
		`c`: []string{},
	})
	test.Equal(t, nil, err2)
	test.Equal(t, "", sql2)
	test.Equal(t, 0, len(params2))
}

func TestMysqlClass_processValues(t *testing.T) {
	mysql := &MysqlClass{
		tagName: `json`,
		logger:  go_logger.DefaultLogger,
	}
	sql, params, err := mysql.processValues("select * from test where a in (?) and b = ?", []interface{}{
		[]string{"123", "456"},
		6345,
	})
	test.Equal(t, nil, err)
	test.Equal(t, "select * from test where a in (?, ?) and b = ?", sql)
	test.Equal(t, 3, len(params))
}

func Test_builderClass_BuildSelectSql(t *testing.T) {
	builder := builderClass{}
	sql, params := builder.MustBuildSelectSql(
		`table`,
		`*`,
		map[string]interface{}{
			"id": "s: in (1,2)",
		},
	)
	test.Equal(t, "select * from table where id in (1,2)", sql)
	test.Equal(t, 0, len(params))

	sql1, params1 := builder.MustBuildSelectSql(
		`table`,
		`*`,
		map[string]interface{}{
			"id": []string{},
		},
	)
	//test.Equal(t, true, strings.HasPrefix(sql, "insert into table "))
	//test.Equal(t, 3, len(params))
	fmt.Println(sql1, params1)
}

func TestMysqlClass_correctSelectStar(t *testing.T) {
	mysql := &MysqlClass{
		tagName: `json`,
		logger:  go_logger.DefaultLogger,
	}
	var testObj struct {
		A   string `json:"a"`
		Abc string `json:"abc"`
	}
	sql := mysql.correctSelectStar(&testObj, `

  
 	select * from abc
`)
	test.Equal(t, "select a,abc from abc\n", sql)
}

func Test_builderClass_BuildSelectSql1(t *testing.T) {
	builder := &builderClass{}
	sql, params, err := builder.BuildSelectSql(
		`table`,
		`*`,
		map[string]interface{}{
			"symbol": strings.Split("", ","),
		},
	)
	test.Equal(t, nil, err)
	test.Equal(t, "select * from table ", sql)
	test.Equal(t, 0, len(params))
}
