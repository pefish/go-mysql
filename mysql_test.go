package go_mysql

import (
	go_interface_logger "github.com/pefish/go-interface-logger"
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
	}, "where ")
	test.Equal(t, true, strings.HasPrefix(sql, "update table set "))
	test.Equal(t, 5, len(params))
}

func TestBuilderClass_BuildInsertSql(t *testing.T) {
	builder := builderClass{}
	sql, params := builder.MustBuildInsertSql(`table`, map[string]interface{}{
		`a`: 123,
		`c`: `hfhd`,
	}, buildInsertSqlOpt{})
	test.Equal(t, true, strings.HasPrefix(sql, "insert into table "))
	test.Equal(t, 2, len(params))
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
	})
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
	})
	test.Equal(t, true, strings.HasPrefix(sql1, "where "))
	test.Equal(t, 2, len(args1))
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
	params, sql, err := builder.buildWhereFromMapInterface(map[string]interface{}{
		`a`: 123,
		`c`: "s:in (35)",
	})
	test.Equal(t, nil, err)
	test.Equal(t, "a = ? and c in (35)", sql)
	test.Equal(t, 1, len(params))
}

func TestMysqlClass_processValues(t *testing.T) {
	mysql := &MysqlClass{
		tagName: `json`,
		logger:  go_interface_logger.DefaultLogger,
	}
	sql, params, err := mysql.processValues("select * from test where a in (?) and b = ?", []interface{}{
		[]string{"123","456"},
		6345,
	})
	test.Equal(t, nil, err)
	test.Equal(t, "select * from test where a in (?, ?) and b = ?", sql)
	test.Equal(t, 3, len(params))
}