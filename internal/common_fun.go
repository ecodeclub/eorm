package internal

import (
	"reflect"
	"unicode"
)

func TableName(table interface{}) (string, []interface{}) {
	t := reflect.TypeOf(table)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	args := []interface{}{}

	num := reflect.ValueOf(table).Elem().NumField()
	for i := 0; i < num; i++ {
		val := reflect.ValueOf(table).Elem().Field(i).Interface()
		args = append(args, val)
	}
	tableName := ""
	if _, ok := t.FieldByName("tableName"); ok {
		paramList := []reflect.Value{}
		resu := reflect.New(t).Method(0).Call(paramList)
		tableName = resu[0].String()
	} else {
		tableName = underscoreName(t.Name())
	}
	return tableName, args
}

func underscoreName(tableName string) string {
	buf := []rune{}
	for i, v := range tableName {
		if unicode.IsUpper(v) {
			if i != 0 {
				buf = append(buf, '_')
			}
			buf = append(buf, unicode.ToLower(v))
		} else {
			buf = append(buf, v)
		}

	}
	return string(buf)
}
