package clickhousex

import (
	"database/sql"
	"fmt"
)

type Column struct {
	Name              string `db:"name"`
	Type              string `db:"type"`
	DefaultType       string `db:"default_type"`
	DefaultExpression string `db:"default_expression"`
	Comment           string `db:"comment"`
	CodecExpression   string `db:"codec_expression"`
	TTLExpression     string `db:"ttl_expression"`
}

func DescTable(pool *sql.DB, database, tableName string) ([]Column, error) {
	rows, e := pool.Query("desc `" + database + "`.`" + tableName + "`")
	if e != nil {
		return nil, e
	}

	out := []Column{}
	for rows.Next() {
		v := Column{}
		e = rows.Scan(&v.Name, &v.Type, &v.DefaultType, &v.DefaultExpression, &v.Comment, &v.CodecExpression, &v.TTLExpression)
		if e != nil {
			break
		}
		out = append(out, v)
	}

	//check err
	if closeErr := rows.Close(); closeErr != nil {
		return nil, fmt.Errorf("rows.Close() err:%w", closeErr)
	}
	if e != nil {
		return nil, e
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}

	return out, nil
}
