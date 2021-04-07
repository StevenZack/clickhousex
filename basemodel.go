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
				reflect.Int,
				reflect.Int64,
				reflect.Int32,
				reflect.Int16,
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

// GetInsertSQL returns insert SQL without returning id
func (b *BaseModel) GetInsertSQL() ([]int, string) {
	builder := new(strings.Builder)
	builder.WriteString(`insert into ` + b.Database + `.` + b.TableName + ` (`)

	values := new(strings.Builder)
	values.WriteString("values (")

	argsIndex := []int{}

	for i, dbTag := range b.dbTags {
		argsIndex = append(argsIndex, i)

		builder.WriteString(dbTag)
		values.WriteString("?")

		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
			values.WriteString(",")
		}

	}

	builder.WriteString(")")
	values.WriteString(")")

	builder.WriteString(values.String())

	return argsIndex, builder.String()
}

// GetSelectSQL returns fieldIndexes, and select SQL
func (b *BaseModel) GetSelectSQL() ([]int, string) {
	builder := new(strings.Builder)
	builder.WriteString(`select `)
	fieldIndexes := []int{}
	for i, dbTag := range b.dbTags {
		builder.WriteString(dbTag)
		fieldIndexes = append(fieldIndexes, i)
		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString(" from " + b.TableName)
	return fieldIndexes, builder.String()
}

// InsertAll inserts vs ([]*struct or []struct type)
func (b *BaseModel) InsertAll(vs interface{}) error {
	//validate
	sliceValue := reflect.ValueOf(vs)
	t := sliceValue.Type()
	if t.Kind() != reflect.Slice {
		return errors.New("Insert value is not an slice type:" + t.String())
	}
	t = t.Elem()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.String() != b.Type.String() {
		return errors.New("Wrong insert type:" + t.String() + " for table " + b.TableName)
	}

	//prepare
	argsIndex, query := b.GetInsertSQL()

	tx, e := b.Pool.Begin()
	if e != nil {
		return e
	}
	stmt, e := tx.Prepare(query)
	if e != nil {
		return e
	}
	defer stmt.Close()

	//exec
	for i := 0; i < sliceValue.Len(); i++ {
		value := sliceValue.Index(i)
		if value.Kind() == reflect.Ptr {
			value = value.Elem()
		}

		//args
		args := []interface{}{}
		for _, j := range argsIndex {
			v := value.Field(j).Interface()
			if value.Field(j).Kind() == reflect.Uint {
				v = uint64(v.(uint))
			}
			args = append(args, v)
		}

		_, e := stmt.Exec(args...)
		if e != nil {
			return fmt.Errorf("insert failed when insert %v:%w", value.Interface(), e)
		}
	}

	//commit
	e = tx.Commit()
	if e != nil {
		return e
	}
	return nil
}

// Find finds a document (*struct type) by id
func (b *BaseModel) Find(id interface{}) (interface{}, error) {
	//scan
	v := reflect.New(b.Type)
	fieldIndexes, query := b.GetSelectSQL()
	fieldArgs := []interface{}{}
	for _, i := range fieldIndexes {
		fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
	}

	query = query + ` where ` + b.dbTags[0] + `=?`
	e := b.Pool.QueryRow(query, id).Scan(fieldArgs...)
	if e != nil {
		if e == sql.ErrNoRows {
			return nil, e
		}
		return nil, fmt.Errorf("%w:%s", e, query)
	}
	return v.Interface(), nil
}

// FindWhere finds a document (*struct type) that matches 'where' condition
func (b *BaseModel) FindWhere(where string, args ...interface{}) (interface{}, error) {
	//where
	where = toWhere(where)

	//scan
	v := reflect.New(b.Type)
	fieldIndexes, query := b.GetSelectSQL()
	query = query + where
	fieldArgs := []interface{}{}
	for _, i := range fieldIndexes {
		fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
	}
	e := b.Pool.QueryRow(query, args...).Scan(fieldArgs...)
	if e != nil {
		if e == sql.ErrNoRows {
			return nil, e
		}
		return nil, fmt.Errorf("%w:%s", e, query)
	}
	return v.Interface(), nil
}

// QueryWhere queries documents ([]*struct type) that matches 'where' condition
func (b *BaseModel) QueryWhere(where string, args ...interface{}) (interface{}, error) {
	where = toWhere(where)

	fieldIndexes, query := b.GetSelectSQL()

	//query
	query = query + where
	rows, e := b.Pool.Query(query, args...)
	if e != nil {
		return nil, fmt.Errorf("%w:%s", e, query)
	}

	vs := reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(b.Type)), 0, 2)
	for rows.Next() {
		v := reflect.New(b.Type)
		fieldArgs := []interface{}{}
		for _, i := range fieldIndexes {
			fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
		}
		e = rows.Scan(fieldArgs...)
		if e != nil {
			break
		}
		vs = reflect.Append(vs, v)
	}

	// check err
	if closeErr := rows.Close(); closeErr != nil {
		return nil, fmt.Errorf("rows.Close() err:%w", closeErr)
	}
	if e != nil {
		return nil, e
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}

	return vs.Interface(), nil
}

func (b *BaseModel) Exists(id interface{}) (bool, error) {
	//scan
	num := 0
	query := `select 1 from ` + b.TableName + ` where ` + b.dbTags[0] + `=? limit 1`
	e := b.Pool.QueryRow(query, id).Scan(&num)
	if e != nil {
		if e == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("%w:%s", e, query)
	}
	return num > 0, nil
}

func (b *BaseModel) ExistsWhere(where string, args ...interface{}) (bool, error) {
	//where
	where = toWhere(where)

	//scan
	num := 0
	query := `select 1 from ` + b.TableName + where + ` limit 1`
	e := b.Pool.QueryRow(query, args...).Scan(&num)
	if e != nil {
		if e == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("%w:%s", e, query)
	}
	return num > 0, nil
}

func (b *BaseModel) CountWhere(where string, args ...interface{}) (int64, error) {
	where = toWhere(where)

	//scan
	var num int64
	query := `select count() as count from ` + b.TableName + where
	e := b.Pool.QueryRow(query, args...).Scan(&num)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return num, nil
}

func (b *BaseModel) UpdateSet(sets string, where string, args ...interface{}) (int64, error) {
	where = toWhere(where)

	query := `update ` + b.TableName + ` set ` + sets + where
	result, e := b.Pool.Exec(query, args...)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return result.RowsAffected()
}

func (b *BaseModel) Clear() error {
	query := `truncate table ` + b.TableName
	_, e := b.Pool.Exec(query)
	if e != nil {
		return fmt.Errorf("%w:%s", e, query)
	}
	return nil
}

func (b *BaseModel) Truncate() error {
	return b.Clear()
}

func (b *BaseModel) Delete(id interface{}) (int64, error) {
	query := `delete from ` + b.TableName + ` where ` + b.dbTags[0] + `=?`
	result, e := b.Pool.Exec(query, id)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return result.RowsAffected()
}

func (b *BaseModel) DeleteWhere(where string, args ...interface{}) (int64, error) {
	where = toWhere(where)

	query := `delete from ` + b.TableName + where
	result, e := b.Pool.Exec(query, args...)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return result.RowsAffected()
}
