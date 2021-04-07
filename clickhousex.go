package clickhousex

import (
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/iancoleman/strcase"
)

func ToTableName(s string) string {
	s = strcase.ToSnake(s)
	return s
}
