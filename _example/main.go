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
	UserId             uint64   `json:"user_id"`
	NewPairId          uint64   `json:"new_pair_id"`
	InitAmount         string   `json:"init_amount"`
	InitTokenAmount    string   `json:"init_token_amount"`
	CurrentTokenAmount string   `json:"current_token_amount"`
	InitTimestamp      uint64   `json:"init_timestamp"`
	Records            []Record `json:"records"` // *Record, map[string]interface{}, []Record, []map[string]interface{}, []*Record
	DbTime
}

type NewPair struct {
	IdType
	Chain               string  `json:"chain"`
	TokenAddress        string  `json:"token_address"`
	TokenDecimals       uint64  `json:"token_decimals"`
	Token0IsWETH        uint64  `json:"token0_is_weth"`
	TokenSymbol         string  `json:"token_symbol"`
	PairAddress         string  `json:"pair_address"`
	PairSymbol          string  `json:"pair_symbol"`
	TradeStartTimestamp uint64  `json:"trade_start_timestamp"`
	InitialEthInPool    string  `json:"initial_eth_in_pool"`
	InitialTokenInPool  string  `json:"initial_token_in_pool"`
	Mark                *string `json:"mark,omitempty"`
	Status              uint64  `json:"status"`
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
		Host:     "52.68.233.193",
		Username: "pefish_me",
		Password: "",
		Database: "pefish_me",
	})
	if err != nil {
		return err
	}

	// newPairs := make([]NewPair, 0)
	// err = mysqlInstance.Select(
	// 	&newPairs,
	// 	&t_mysql.SelectParams{
	// 		TableName: "new_pair",
	// 		Select:    "*",
	// 		Where:     "status = 1",
	// 		Limit:     5,
	// 		OrderBy: &t_mysql.OrderByType{
	// 			Order: t_mysql.OrderType_DESC,
	// 			Col:   "id",
	// 		},
	// 	},
	// )
	// if err != nil {
	// 	return err
	// }

	newPairPoses := make([]NewPairPos, 0)
	err = mysqlInstance.Select(
		&newPairPoses,
		&t_mysql.SelectParams{
			TableName: "new_pair_pos",
			Select:    "*",
			Limit:     5,
			OrderBy: &t_mysql.OrderByType{
				Order: t_mysql.OrderType_DESC,
				Col:   "id",
			},
		},
	)
	if err != nil {
		return err
	}
	for _, newPairPos := range newPairPoses {
		for _, record := range newPairPos.Records {
			fmt.Println(newPairPos.Id, record.TokenAmount)
		}
	}

	var newPairPos NewPairPos
	_, err = mysqlInstance.SelectFirst(
		&newPairPos,
		&t_mysql.SelectParams{
			TableName: "new_pair_pos",
			Select:    "*",
			Where:     "id = 92",
		},
	)
	if err != nil {
		return err
	}

	fmt.Println(newPairPos.Id, newPairPos.Records[0].TokenAmount)
	// for _, newPair := range newPairs {
	// 	fmt.Println(newPair.Id, *newPair.Mark)
	// 	// for _, record := range task.Records {
	// 	// 	fmt.Println(record.Amount)
	// 	// }
	// }

	return nil
}
