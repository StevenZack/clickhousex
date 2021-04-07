package main

import (
	"fmt"
	"log"
	"time"

	"github.com/StevenZack/clickhousex"
)

type User struct {
	Id         uint      `db:"id"`
	Name       string    `db:"name" index:""`
	CreateTime time.Time `db:"create_time"`
}

func main() {
	dsn := "tcp://localhost:9000?database=default"
	m, e := clickhousex.NewBaseModel(dsn, User{})
	if e != nil {
		log.Fatal(e)
	}

	fmt.Println(m)
}
