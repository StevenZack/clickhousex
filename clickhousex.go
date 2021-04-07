package clickhousex

import (
	"strings"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/StevenZack/tools/strToolkit"
	"github.com/iancoleman/strcase"
)

func ToTableName(s string) string {
	s = strcase.ToSnake(s)
	return s
}

func toWhere(where string) string {
	where = strToolkit.TrimStart(where, " ")
	if where != "" && !strings.HasPrefix(where, "where") {
		where = " where " + where
	}
	return where
}
