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

func init() {
	log.SetFlags(log.Lshortfile)
}

func main() {
	dsn := "tcp://localhost:9000?database=default"
	m, e := clickhousex.NewBaseModel(dsn, User{})
	if e != nil {
		log.Println(e)
		return
	}

	vs, e := m.QueryWhere("id>1")
	if e != nil {
		log.Println(e)
		return
	}

	data := vs.([]*User)
	for _, v := range data {
		fmt.Println(*v)
	}
}
