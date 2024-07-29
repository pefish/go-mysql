package main

import (
	"fmt"
	"log"
	"time"

	i_logger "github.com/pefish/go-interface/i-logger"
	i_mysql "github.com/pefish/go-interface/i-mysql"
	t_mysql "github.com/pefish/go-interface/t-mysql"
	go_mysql "github.com/pefish/go-mysql"
)

type DbTime struct {
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type IdType struct {
	Id uint64 `json:"id,omitempty"`
}

type Task struct {
	IdType
	Name     string                 `json:"name"`
	Desc     string                 `json:"desc"`
	Interval uint64                 `json:"interval"`
	Data     map[string]interface{} `json:"data"`
	Status   uint64                 `json:"status,omitempty"`
	DbTime
}

func main() {
	err := do()
	if err != nil {
		log.Fatal(err)
	}

}

func do() error {
	var mysqlInstance i_mysql.IMysql = go_mysql.NewMysqlInstance(&i_logger.DefaultLogger)
	err := mysqlInstance.ConnectWithConfiguration(t_mysql.Configuration{
		Host:     "",
		Username: "",
		Password: "",
		Database: "shadouzuo",
	})
	if err != nil {
		return err
	}

	tasks := make([]Task, 0)

	err = go_mysql.MysqlInstance.Select(
		&tasks,
		&t_mysql.SelectParams{
			TableName: "task",
			Select:    "*",
		},
	)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		fmt.Printf("%s, %s, %#v\n", task.Name, task.Desc, task.Data)
	}

	return nil
}
