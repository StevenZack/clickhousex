package clickhousex

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"reflect"
)

type BaseModel struct {
	Type      reflect.Type
	Dsn       string
	Pool      *sql.DB
	Database  string
	TableName string
	dbTags    []string
	chTypes   []string
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
		chType, e := ToChPrimitiveType(field.Type)
		if e != nil {
			log.Println(e)
			return nil, false, fmt.Errorf("Field %s:%w", field.Name, e)
		}

		m.dbTags = append(m.dbTags, dbTag)
		m.chTypes = append(m.chTypes, chType)
	}

	
	return m, created, nil
}
