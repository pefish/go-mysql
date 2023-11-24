package go_mysql

import (
	"fmt"
	"github.com/pefish/go-test"
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
	go_test_.Equal(t, true, strings.HasPrefix(sql, "update table set "))
	go_test_.Equal(t, 5, len(params))

	_, params1 := builder.MustBuildUpdateSql(`table`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	}, `where b = ? and f = ?`, 23, 19)
	//go_test_.Equal(t, "update table set a = ?,c = ? where b = ? and f = ?", sql1)
	go_test_.Equal(t, 4, len(params1))
}

func TestBuilderClass_BuildInsertSql(t *testing.T) {
	builder := builderClass{}
	sql, params := builder.MustBuildInsertSql(`table`, map[string]interface{}{
		`a`: 123,
		`c`: `s: = hfhd`,
	}, buildInsertSqlOpt{})
	go_test_.Equal(t, true, strings.HasPrefix(sql, "insert into table "))
	go_test_.Equal(t, 1, len(params))

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
	go_test_.Equal(t, true, strings.HasPrefix(sql1, "INSERT INTO table "))
	go_test_.Equal(t, 9, len(params1))

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
	go_test_.Equal(t, true, strings.HasPrefix(sql2, "INSERT INTO table "))
	go_test_.Equal(t, 4, len(params2))
}

func TestBuilderClass_BuildCountSql(t *testing.T) {
	builder := builderClass{}
	sql, params := builder.MustBuildCountSql(`table`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	})
	go_test_.Equal(t, true, strings.HasPrefix(sql, "select count(*) as count from table where "))
	go_test_.Equal(t, 2, len(params))
}

func TestBuilderClass_BuildSumSql(t *testing.T) {
	builder := builderClass{}
	sql, params := builder.MustBuildSumSql(`table`, `aa`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	})
	go_test_.Equal(t, true, strings.HasPrefix(sql, "select sum(aa) as sum from table where "))
	go_test_.Equal(t, 2, len(params))
}

func TestBuilderClass_BuildWhere(t *testing.T) {
	builder := builderClass{}
	args, sql := builder.MustBuildWhere(map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
		`b`: `s:in ("haha","hehe")`,
	}, nil)
	go_test_.Equal(t, true, strings.HasPrefix(sql, "where "))
	go_test_.Equal(t, 2, len(args))

	args1, sql1 := builder.MustBuildWhere(struct {
		A string  `json:"a"`
		B string  `json:"b"`
		C *string `json:"c"`
	}{
		A: `35`,
		B: `765`,
		C: nil,
	}, nil)
	//fmt.Println(args1, sql1)
	go_test_.Equal(t, true, strings.HasPrefix(sql1, "where "))
	go_test_.Equal(t, 2, len(args1))

	args2, sql2 := builder.MustBuildWhere(`where a = ? and b = ?`, []interface{}{"1", "2"})
	go_test_.Equal(t, "where a = ? and b = ?", sql2)
	go_test_.Equal(t, 2, len(args2))

}

func TestBuilderClass_BuildInsertSql2(t *testing.T) {
	type Test2 struct {
		C string `json:"c"`
	}

	type Test1 struct {
		A     string `json:"a"`
		B     uint64 `json:"b"`
		Test2 `json:"test2"`
	}
	test1 := Test1{
		B: 123,
		A: "456",
	}
	_, params := builder.MustBuildInsertSql(`table`, test1, buildInsertSqlOpt{})
	//fmt.Println(sql, params)
	//go_test_.Equal(t, true, strings.HasPrefix(sql, "INSERT INTO table "))
	go_test_.Equal(t, 2, len(params))
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
	go_test_.Equal(t, true, strings.HasPrefix(sql, "insert into table "))
	go_test_.Equal(t, 3, len(params))
}

func Test_builderClass_buildWhereFromMapInterface(t *testing.T) {
	builder := builderClass{}
	params, sql, err := builder.buildWhereFromMap(map[string]interface{}{
		`a`: 123,
		`c`: "s: in (35)",
	})
	go_test_.Equal(t, nil, err)
	go_test_.Equal(t, "a = ? and c in (35)", sql)
	go_test_.Equal(t, 1, len(params))
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
	go_test_.Equal(t, nil, err)
	go_test_.In(t, []interface{}{
		"a = ? and c in (?,?,?)",
		"c in (?,?,?) and a = ?",
	}, sql)
	go_test_.Equal(t, 4, len(params))

	params1, sql1, err1 := builder.buildWhereFromMap(map[string]interface{}{
		`a`: 123,
		`c`: []interface{}{},
	})
	go_test_.Equal(t, nil, err1)
	go_test_.Equal(t, "a = ?", sql1)
	go_test_.Equal(t, 1, len(params1))

	params2, sql2, err2 := builder.buildWhereFromMap(map[string]interface{}{
		`c`: []string{},
	})
	go_test_.Equal(t, nil, err2)
	go_test_.Equal(t, "", sql2)
	go_test_.Equal(t, 0, len(params2))
}

func TestMysqlClass_processValues(t *testing.T) {
	mysql := NewMysqlInstance()
	sql, params, err := mysql.processValues("select * from test where a in (?) and b = ?", []interface{}{
		[]string{"123", "456"},
		6345,
	})
	go_test_.Equal(t, nil, err)
	go_test_.Equal(t, "select * from test where a in (?, ?) and b = ?", sql)
	go_test_.Equal(t, 3, len(params))
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
	go_test_.Equal(t, "select * from table where id in (1,2)", sql)
	go_test_.Equal(t, 0, len(params))

	sql1, params1 := builder.MustBuildSelectSql(
		`table`,
		`*`,
		map[string]interface{}{
			"id": []string{},
		},
	)
	//go_test_.Equal(t, true, strings.HasPrefix(sql, "insert into table "))
	//go_test_.Equal(t, 3, len(params))
	fmt.Println(sql1, params1)
}

func TestMysqlClass_correctSelectStar(t *testing.T) {
	mysql := NewMysqlInstance()
	var testObj struct {
		A   string `json:"a"`
		Abc string `json:"abc"`
	}
	sql := mysql.correctSelectStar(&testObj, `

  
 	select * from abc
`)
	go_test_.Equal(t, "select a,abc from abc\n", sql)
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
	go_test_.Equal(t, nil, err)
	go_test_.Equal(t, "select * from table ", sql)
	go_test_.Equal(t, 0, len(params))

	sql1, params1, err1 := builder.BuildSelectSql(
		`table`,
		`*`,
		map[string]interface{}{
			"symbol": strings.Split("test", ","),
		},
	)
	go_test_.Equal(t, nil, err1)
	go_test_.Equal(t, "select * from table where symbol in (?)", sql1)
	go_test_.Equal(t, 1, len(params1))
}

func Test_builderClass_structToMap(t *testing.T) {
	type Nest struct {
		B string `json:"b"`
	}
	type Test struct {
		A string `json:"a,b"`
		Nest
	}

	mysql := &builderClass{}
	result := make(map[string]interface{}, 0)
	err := mysql.structToMap(Test{
		A:    "aaaaa",
		Nest: Nest{B: "bbbbb"},
	}, result)
	go_test_.Equal(t, nil, err)
	go_test_.Equal(t, "aaaaa", result["a"])
	go_test_.Equal(t, "bbbbb", result["b"])
}
