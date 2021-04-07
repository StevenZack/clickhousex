package clickhousex

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"strconv"
	"strings"
)

type BaseModel struct {
	Type      reflect.Type
	Dsn       string
	Pool      *sql.DB
	Database  string
	TableName string

	dbTags  []string
	chTypes []string
	orderBy string
}

func NewBaseModel(dsn string, data interface{}) (*BaseModel, error) {
	model, _, e := NewBaseModelWithCreated(dsn, data)
	return model, e
}

func NewBaseModelWithCreated(dsn string, data interface{}) (*BaseModel, bool, error) {
	created := false
	t := reflect.TypeOf(data)

	url, e := url.Parse(dsn)
	if e != nil {
		log.Println(e)
		return nil, false, e
	}
	query := url.Query()

	m := &BaseModel{
		Type:      t,
		Dsn:       dsn,
		Database:  query.Get("database"),
		TableName: ToTableName(t.Name()),
	}
	m.Pool, e = sql.Open("clickhouse", dsn)
	if e != nil {
		log.Println(e)
		return nil, false, e
	}

	//check data
	if t.Kind() == reflect.Ptr {
		return nil, false, errors.New("data must be struct type")
	}
	if t.NumField() == 0 {
		return nil, false, errors.New("No field in data struct")
	}

	indexes := make(map[string]string)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if i == 0 {
			switch field.Type.Kind() {
			case reflect.Uint,
				reflect.Uint64,
				reflect.Uint32,
				reflect.Uint16,
				reflect.String:
			default:
				return nil, false, errors.New("The first field " + field.Name + "'s type must be one of uint,uint32,uint64,uint16,string")
			}
		}

		//dbTag
		dbTag, ok := field.Tag.Lookup("db")
		if !ok {
			return nil, false, errors.New("field " + field.Name + " has no `db` tag specified")
		}
		if i == 0 && dbTag != "id" {
			return nil, false, errors.New("The first field's `db` tag must be id")
		}

		//index
		if index, ok := field.Tag.Lookup("index"); ok {
			indexes[dbTag] = index
		}

		//chType
		chType, e := ToChType(field.Type)
		if e != nil {
			log.Println(e)
			return nil, false, fmt.Errorf("Field %s:%w", field.Name, e)
		}

		m.dbTags = append(m.dbTags, dbTag)
		m.chTypes = append(m.chTypes, chType)

		//order by
		if _, ok := field.Tag.Lookup("orderby"); ok {
			m.orderBy = dbTag
		}
	}

	columns, e := DescTable(m.Pool, m.Database, m.TableName)
	if e != nil && !strings.Contains(e.Error(), "doesn't exist") {
		log.Println(e)
		return nil, false, e
	}
	if len(columns) == 0 {
		//create table
		e = m.createTable(indexes)
		if e != nil {
			log.Println(e)
			return nil, false, e
		}
		created = true
	} else {
		// remote column check
		if len(m.dbTags) != len(columns) {
			return nil, false, errors.New("Inconsistent field number with remote columns: local=" + strconv.Itoa(len(m.dbTags)) + ", remote=" + strconv.Itoa(len(columns)))
		}
		for i, db := range m.dbTags {
			column := columns[i]
			if db != column.Name {
				return nil, false, errors.New("Field[" + strconv.Itoa(i) + "] " + db + " name doesn't match remote column:" + column.Name)
			}

			dbType := m.chTypes[i]
			remoteType := column.Type
			if dbType != remoteType {
				return nil, false, errors.New("Field[" + strconv.Itoa(i) + "] " + db + "'s type '" + dbType + "' doesn't match remote type:" + remoteType)
			}
		}
	}
	return m, created, nil
}

func (b *BaseModel) createTable(indexes map[string]string) error {
	query := b.GetCreateTableSQL(indexes)
	_, e := b.Pool.Exec(query)
	if e != nil {
		return fmt.Errorf("%w: %s", e, query)
	}
	return nil
}

func (b *BaseModel) GetCreateTableSQL(indexes map[string]string) string {
	builder := new(strings.Builder)
	builder.WriteString(`create table ` + b.Database + `.` + b.TableName + ` (`)
	for i, dbTag := range b.dbTags {
		builder.WriteString(dbTag + " ")
		builder.WriteString(b.chTypes[i])
		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
		}
	}
	for k := range indexes {
		builder.WriteString(`,index index_` + k + ` ` + k + ` type minmax granularity 32`)
	}
	builder.WriteString(`) engine=MergeTree()`)
	if b.orderBy != "" {
		builder.WriteString(` order by ` + b.orderBy)
	}
	builder.WriteString(` primary key ` + b.dbTags[0])
	return builder.String()
}
