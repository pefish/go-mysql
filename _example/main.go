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

type Record struct {
	TokenAmount float64 `json:"token_amount"`
	Type        string  `json:"type"`
	Amount      float64 `json:"amount"`
}

type NewPairPos struct {
	IdType
	UserId             uint64  `json:"user_id"`
	NewPairId          uint64  `json:"new_pair_id"`
	InitAmount         string  `json:"init_amount"`
	InitTokenAmount    string  `json:"init_token_amount"`
	CurrentTokenAmount string  `json:"current_token_amount"`
	InitTimestamp      uint64  `json:"init_timestamp"`
	Records            *Record `json:"records"` // *Record, map[string]interface{}, []Record, []map[string]interface{}, []*Record
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
		Username: "pefish_me",
		Password: "",
		Database: "pefish_me",
	})
	if err != nil {
		return err
	}

	tasks := make([]NewPairPos, 0)

	err = mysqlInstance.Select(
		&tasks,
		&t_mysql.SelectParams{
			TableName: "new_pair_pos",
			Select:    "*",
		},
	)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		fmt.Println(task.Records)
		// for _, record := range task.Records {
		// 	fmt.Println(record.Amount)
		// }
	}

	return nil
}
