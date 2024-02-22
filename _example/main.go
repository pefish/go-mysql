package main

import (
	"fmt"
	"log"
	"time"

	go_logger "github.com/pefish/go-logger"
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
	go_mysql.MysqlInstance.SetLogger(go_logger.Logger)
	err := go_mysql.MysqlInstance.ConnectWithConfiguration(go_mysql.Configuration{
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
		&go_mysql.SelectParams{
			TableName: "task",
			Select:    "*",
		},
	)
	if err != nil {
		go_logger.Logger.Error(err)
		return err
	}

	for _, task := range tasks {
		fmt.Printf("%s, %s, %#v\n", task.Name, task.Desc, task.Data)
	}

	return nil
}
