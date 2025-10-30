package main

import (
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
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

type CreateTokenRecord struct {
	IdType
	TokenAddress        string         `json:"token_address"`
	TokenPriv           string         `json:"token_priv"`
	URI                 string         `json:"uri"`
	FatherAddressIndex  uint64         `json:"father_address_index"`
	CreatorAddressIndex uint64         `json:"creator_address_index"`
	CreateTimestamp     uint64         `json:"create_timestamp"`
	SubAddresses        []int          `json:"sub_addresses"`
	Profit              string         `json:"profit"`
	Status              uint64         `json:"status"`
	ErrInfo             *string        `json:"err_info,omitempty"`
	ProcessResult       map[string]any `json:"process_result"`
	DbTime
}

type FourmemeToken struct {
	IdType
	Name            string `json:"name"`
	Address         string `json:"address"`
	Symbol          string `json:"symbol"`
	Creator         string `json:"creator"`
	CreateTimestamp uint64 `json:"create_timestamp"`
	CreateBlock     uint64 `json:"create_block"`
	DbTime
}

func main() {
	envMap, _ := godotenv.Read("./.env")
	for k, v := range envMap {
		os.Setenv(k, v)
	}

	err := do()
	if err != nil {
		log.Fatal(err)
	}

}

func do() error {
	var mysqlInstance i_mysql.IMysql = go_mysql.NewMysqlInstance(&i_logger.DefaultLogger)
	err := mysqlInstance.ConnectWithConfiguration(t_mysql.Configuration{
		Host:     os.Getenv("DB_HOST"),
		Username: os.Getenv("DB_USER"),
		Password: os.Getenv("DB_PASS"),
		Database: os.Getenv("DB_DB"),
	})
	if err != nil {
		return err
	}

	fourmemeTokens := make([]FourmemeToken, 0)
	err = mysqlInstance.Select(
		&fourmemeTokens,
		&t_mysql.SelectParams{
			TableName: "fourmeme_token",
			Select:    "*",
			Where:     `name like ?`,
			OrderBy: &t_mysql.OrderByType{
				Col:   "create_timestamp",
				Order: t_mysql.OrderType_DESC,
			},
		},
		"%cz%",
	)
	if err != nil {
		return err
	}
	for _, token := range fourmemeTokens {
		log.Printf("token: %+v\n", token)
	}

	return nil
}
