package clickhousex

import (
	"errors"
	"reflect"
)

func ToChType(t reflect.Type) (string, error) {
	switch t.Kind() {
	case reflect.Slice:
		v, e := ToChPrimitiveType(t.Elem())
		if e != nil {
			return "", e
		}
		return "Array(" + v + ")", nil
	case reflect.Struct:
		switch t.String() {
		case "time.Time":
			return "DateTime('Asia/Shanghai')", nil
		default:
			return "", errors.New("unsupported data type:" + t.String())
		}
	default:
		v, e := ToChPrimitiveType(t)
		if e != nil {
			return "", e
		}
		return v, nil
	}
}

func ToChPrimitiveType(t reflect.Type) (string, error) {
	switch t.Kind() {
	case reflect.Int, reflect.Int64:
		return "Int64", nil
	case reflect.Int32:
		return "Int32", nil
	case reflect.Int16:
		return "Int16", nil
	case reflect.Uint, reflect.Uint64:
		return "UInt64", nil
	case reflect.Uint32:
		return "UInt32", nil
	case reflect.Uint16:
		return "UInt16", nil
	case reflect.Float32:
		return "Float32", nil
	case reflect.Float64:
		return "Float64", nil
	case reflect.String:
		return "String", nil
	case reflect.Bool:
		return "Boolean", nil
	default:
		return "", errors.New("unsupported for primitive type:" + t.String())
	}
}
