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
	Name   string                 `json:"name"`
	Chain  string                 `json:"chain"`
	Params map[string]interface{} `json:"params"`
	UserId uint64                 `json:"user_id"`
	Status uint64                 `json:"status"`
	Result map[string]interface{} `json:"result"`
	TgInfo map[string]interface{} `json:"tg_info"`
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
		Database: "1",
	})
	if err != nil {
		return err
	}

	tasks := make([]Task, 0)

	err = mysqlInstance.Select(
		&tasks,
		&t_mysql.SelectParams{
			TableName: "task",
			Select:    "*",
			Where: map[string]interface{}{
				"name":   tasks,
				"status": 0,
				"chain":  "BaseTestnet",
			},
		},
	)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		fmt.Println(task.Name)
		fmt.Println(task.Params)
		fmt.Println(task.Result)
		if task.Result == nil {
			fmt.Println(11)
		}
	}

	return nil
}
