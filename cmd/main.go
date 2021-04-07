package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/StevenZack/clickhousex"
)

func main() {
	dsn := "tcp://localhost:9000?database=default"
	pool, e := sql.Open("clickhouse", dsn)
	if e != nil {
		log.Println(e)
		return
	}

	cs, e := clickhousex.DescTable(pool, "default", "user")
	if e != nil {
		log.Println(e)
		return
	}
	for _,c:=range cs{
		fmt.Println(c)
	}
}
