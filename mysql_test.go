package go_mysql

import (
	"fmt"
	"strings"
	"testing"
	"time"

	i_logger "github.com/pefish/go-interface/i-logger"
	t_mysql "github.com/pefish/go-interface/t-mysql"
	go_test_ "github.com/pefish/go-test"
)

type IdType struct {
	Id uint64 `json:"id,omitempty"`
}

type DbTime struct {
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type Test struct {
	IdType
	A string  `json:"a"`
	B uint64  `json:"b"`
	C *string `json:"c,omitempty"`
	DbTime
}

func TestBuilderClass_BuildUpdateSql(t *testing.T) {
	builder := builderClass{}
	sql, params, err := builder.buildUpdateSql(
		&t_mysql.UpdateParams{
			TableName: "table",
			Update: map[string]interface{}{
				`a`: 123,
				`c`: `hfhd`,
			},
			Where: []map[string]interface{}{
				{
					`b`:    65,
					`bghf`: `352352`,
				},
				{
					`bnn`: `345`,
				},
			},
		},
	)
	fmt.Println(sql, params)
	go_test_.Equal(t, nil, err)
	go_test_.Equal(t, true, strings.HasPrefix(sql, "update `table` set "))
	go_test_.Equal(t, 5, len(params))

	_, params1, err := builder.buildUpdateSql(
		&t_mysql.UpdateParams{
			TableName: "table",
			Update: map[string]interface{}{
				`a`: 123,
				`c`: `hfhd`,
			},
			Where: `where b = ? and f = ?`,
		},
		23,
		19,
	)
	go_test_.Equal(t, nil, err)
	//go_test_.Equal(t, "update table set a = ?,c = ? where b = ? and f = ?", sql1)
	go_test_.Equal(t, 4, len(params1))

	_, params2, err := builder.buildUpdateSql(
		&t_mysql.UpdateParams{
			TableName: "table",
			Update:    "a = ?",
			Where:     `b = ? and f = ?`,
		},
		11,
		23,
		19,
	)
	// fmt.Println(sql2, params2)
	go_test_.Equal(t, nil, err)
	// go_test_.Equal(t, "update table set a = ?,c = ? where b = ? and f = ?", sql2)
	go_test_.Equal(t, 3, len(params2))
}

func TestBuilderClass_BuildInsertSql(t *testing.T) {
	builder := builderClass{}
	// sql, params, err := builder.buildInsertSql(`table`, map[string]interface{}{
	// 	`a`: 123,
	// 	`c`: `hfhd`,
	// })
	// fmt.Println(sql, params)
	// go_test_.Equal(t, nil, err)
	// go_test_.Equal(t, true, strings.HasPrefix(sql, "insert into `table` "))
	// go_test_.Equal(t, 2, len(params))

	sql3, params3, err := builder.buildInsertSql(`table`, Test{
		B: 345,
		A: `aaa`,
	})
	go_test_.Equal(t, nil, err)
	fmt.Println(sql3, params3)
	go_test_.Equal(t, true, strings.HasPrefix(strings.ToLower(sql3), "insert into `table` "))
	go_test_.Equal(t, 2, len(params3))

	// sql1, params1, err := builder.buildInsertSql(`table`, []map[string]interface{}{
	// 	{
	// 		`a`: 123,
	// 		`c`: `hfhd`,
	// 		`b`: 431,
	// 	},
	// 	{
	// 		`a`: 345,
	// 		`c`: `aaa`,
	// 		`b`: 524,
	// 	},
	// 	{
	// 		`a`: 444,
	// 		`c`: `qqq`,
	// 		`b`: 111,
	// 	},
	// })
	// fmt.Println(sql1, params1)
	// go_test_.Equal(t, nil, err)
	// go_test_.Equal(t, true, strings.HasPrefix(strings.ToLower(sql1), "insert into `table` "))
	// go_test_.Equal(t, 9, len(params1))

	// tmpStr := "123"
	// sql2, params2, err := builder.buildInsertSql(`table`, []Test{
	// 	{
	// 		A: `hfhd`,
	// 		B: 123,
	// 		C: &tmpStr,
	// 	},
	// 	{
	// 		A: `aaa`,
	// 		B: 345,
	// 		C: &tmpStr,
	// 	},
	// })
	// go_test_.Equal(t, nil, err)
	// fmt.Println(sql2, params2)
	// go_test_.Equal(t, true, strings.HasPrefix(strings.ToLower(sql2), "insert into `table` "))
	// go_test_.Equal(t, 6, len(params2))

	// type Test2 struct {
	// 	C string `json:"c,omitempty"`
	// }

	// type Test1 struct {
	// 	A     string `json:"a"`
	// 	B     uint64 `json:"b"`
	// 	Test2 `json:"test2"`
	// }
	// test1 := Test1{
	// 	B: 123,
	// 	A: "456",
	// }
	// sql4, params4, err := builder.buildInsertSql(`table`, test1)
	// go_test_.Equal(t, nil, err)
	// fmt.Println(sql4, params4)
	// go_test_.Equal(t, true, strings.HasPrefix(strings.ToLower(sql4), "insert into `table` "))
	// go_test_.Equal(t, 2, len(params4))

	// type Test3 struct {
	// 	A map[string]interface{} `json:"a"`
	// }
	// test3 := Test3{
	// 	A: map[string]interface{}{
	// 		"test": "123",
	// 	},
	// }
	// sql5, params5, err := builder.buildInsertSql(`table`, test3)
	// go_test_.Equal(t, nil, err)
	// fmt.Println(sql5, params5)
	// go_test_.Equal(t, true, strings.HasPrefix(strings.ToLower(sql5), "insert into `table` "))
	// go_test_.Equal(t, 1, len(params5))
}

func TestBuilderClass_BuildWhere(t *testing.T) {
	builder := builderClass{}
	args, sql, err := builder.buildWhere(map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
		`b`: `s:in ("haha","hehe")`,
	}, nil)
	go_test_.Equal(t, nil, err)
	go_test_.Equal(t, true, strings.HasPrefix(sql, "where "))
	go_test_.Equal(t, 2, len(args))

	args1, sql1, err := builder.buildWhere(struct {
		A string  `json:"a"`
		B string  `json:"b"`
		C *string `json:"c,omitempty"`
	}{
		A: `35`,
		B: `765`,
		C: nil,
	}, nil)
	go_test_.Equal(t, nil, err)
	//fmt.Println(args1, sql1)
	go_test_.Equal(t, true, strings.HasPrefix(sql1, "where "))
	go_test_.Equal(t, 2, len(args1))

	args2, sql2, err := builder.buildWhere("`a` = ? and `b` = ?", []interface{}{"1", "2"})
	go_test_.Equal(t, nil, err)
	go_test_.Equal(t, "where `a` = ? and `b` = ?", sql2)
	go_test_.Equal(t, 2, len(args2))

}

func Test_builderClass_buildWhereFromMapInterface(t *testing.T) {
	builder := builderClass{}
	params, sql := builder.buildWhereFromMap(map[string]interface{}{
		`a`: 123,
		`c`: "s: in (35)",
	})
	go_test_.Equal(t, true, strings.Contains(sql, "`a` = ?"))
	go_test_.Equal(t, true, strings.Contains(sql, "`c` in (35)"))
	go_test_.Equal(t, 1, len(params))
}

func Test_builderClass_buildWhereFromMapInterface1(t *testing.T) {
	builder := builderClass{}
	params, sql := builder.buildWhereFromMap(map[string]interface{}{
		`a`: 123,
		`c`: []interface{}{
			35,
			27,
			11,
		},
	})
	go_test_.In(t, []interface{}{
		"`a` = ? and `c` in (?,?,?)",
		"`c` in (?,?,?) and `a` = ?",
	}, sql)
	go_test_.Equal(t, 4, len(params))

	params1, sql1 := builder.buildWhereFromMap(map[string]interface{}{
		`a`: 123,
		`c`: []interface{}{},
	})
	go_test_.Equal(t, "`a` = ?", sql1)
	go_test_.Equal(t, 1, len(params1))

	params2, sql2 := builder.buildWhereFromMap(map[string]interface{}{
		`c`: []string{},
	})
	go_test_.Equal(t, "", sql2)
	go_test_.Equal(t, 0, len(params2))
}

func TestMysqlClass_processValues(t *testing.T) {
	mysql := NewMysqlInstance(&i_logger.DefaultLogger)
	sql, params, err := mysql.processValues("select * from `test` where `a` in (?) and `b` = ?", []interface{}{
		[]string{"123", "456"},
		6345,
	})
	go_test_.Equal(t, nil, err)
	go_test_.Equal(t, "select * from `test` where `a` in (?, ?) and `b` = ?", sql)
	go_test_.Equal(t, 3, len(params))
}

func Test_builderClass_BuildSelectSql(t *testing.T) {
	builder := builderClass{}
	sql, params, err := builder.buildSelectSql(
		&t_mysql.SelectParams{
			TableName: "table",
			Select:    "*",
			Where: map[string]interface{}{
				"id": "s: in (1,2)",
			},
		},
	)
	go_test_.Equal(t, nil, err)
	go_test_.Equal(t, "select * from `table` where `id` in (1,2)", sql)
	go_test_.Equal(t, 0, len(params))

	sql1, params1, err := builder.buildSelectSql(
		&t_mysql.SelectParams{
			TableName: "table",
			Select:    "*",
			Where: map[string]interface{}{
				"id": []string{},
			},
		},
	)
	go_test_.Equal(t, nil, err)
	//go_test_.Equal(t, true, strings.HasPrefix(sql, "insert into table "))
	//go_test_.Equal(t, 3, len(params))
	fmt.Println(sql1, params1)
}

func Test_builderClass_BuildSelectSql1(t *testing.T) {
	builder := &builderClass{}
	sql, params, err := builder.buildSelectSql(
		&t_mysql.SelectParams{
			TableName: "table",
			Select:    "*",
			Where: map[string]interface{}{
				"symbol": strings.Split("", ","),
			},
		},
	)
	go_test_.Equal(t, nil, err)
	go_test_.Equal(t, "select * from `table` ", sql)
	go_test_.Equal(t, 0, len(params))

	sql1, params1, err1 := builder.buildSelectSql(
		&t_mysql.SelectParams{
			TableName: "table",
			Select:    "*",
			Where: map[string]interface{}{
				"symbol": strings.Split("test", ","),
			},
		},
	)
	go_test_.Equal(t, nil, err1)
	go_test_.Equal(t, "select * from `table` where `symbol` in (?)", sql1)
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
	result := make(map[string]interface{})
	err := mysql.structToMap(Test{
		A:    "aaaaa",
		Nest: Nest{B: "bbbbb"},
	}, result)
	go_test_.Equal(t, nil, err)
	go_test_.Equal(t, "aaaaa", result["a"])
	go_test_.Equal(t, "bbbbb", result["b"])
}

func Test_builderClass_buildInsertSql(t *testing.T) {

	datas := make([]*Test, 0)
	datas = append(datas, &Test{
		A: "a",
		B: 2,
	}, &Test{
		A: "b",
		B: 3,
	})
	mysql := &builderClass{}
	sql, args, err := mysql.buildInsertSql(
		"table",
		datas,
	)
	go_test_.Equal(t, nil, err)
	//fmt.Println(sql)
	//fmt.Println(args)
	go_test_.Equal(t, true, strings.HasPrefix(strings.ToLower(sql), "insert into `table`"))
	go_test_.Equal(t, 4, len(args))

	sql, args, err = mysql.buildInsertSql(
		"table",
		&Test{
			A: "b",
			B: 3,
		},
	)
	go_test_.Equal(t, nil, err)
	// fmt.Println(sql)
	// fmt.Println(args)
	go_test_.Equal(t, true, strings.HasPrefix(strings.ToLower(sql), "insert into `table`"))
	go_test_.Equal(t, 2, len(args))
}
