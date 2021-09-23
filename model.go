package eql

import (
	"reflect"
	"strings"
	"sync"

	"github.com/gotomicro/eql/internal"
)

// TableMeta represents data model, or a table
type TableMeta struct {
	tableName string
	columns   []*ColumnMeta
	fieldMap  map[string]*ColumnMeta
	typ       reflect.Type
}

// ColumnMeta represents model's field, or column
type ColumnMeta struct {
	columnName      string
	fieldName       string
	typ             reflect.Type
	isPrimaryKey    bool
	isAutoIncrement bool
}

type tableMetaOption func(meta *TableMeta)

type MetaRegistry interface {
	Get(table interface{}, opts ...tableMetaOption) (*TableMeta, error)
	Register(table interface{}, opts ...tableMetaOption) (*TableMeta, error)
}

var defaultMetaRegistry = &tagMetaRegistry{}

type tagMetaRegistry struct {
	metas sync.Map
}

func (t *tagMetaRegistry) Get(table interface{}) (*TableMeta, error) {

	if v, ok := t.metas.Load(reflect.TypeOf(table)); ok {
		return v.(*TableMeta), nil
	}
	return t.Register(table)
}

func (t *tagMetaRegistry) Register(table interface{}, opts ...tableMetaOption) (*TableMeta, error) {
	rtype := reflect.TypeOf(table)
	v := rtype.Elem()
	columnMetas := []*ColumnMeta{}
	lens := v.NumField()
	fieldMap := make(map[string]*ColumnMeta, lens)
	for i := 0; i < lens; i++ {
		structField := v.Field(i)
		tag := structField.Tag.Get("eql")
		isAuto := strings.Contains(tag, "auto_increment")
		isKey := strings.Contains(tag, "primary_key")
		columnMeta := &ColumnMeta{
			columnName:      internal.UnderscoreName(structField.Name),
			fieldName:       structField.Name,
			typ:             structField.Type,
			isAutoIncrement: isAuto,
			isPrimaryKey:    isKey,
		}
		columnMetas = append(columnMetas, columnMeta)
		fieldMap[columnMeta.fieldName] = columnMeta
	}
	TableMeta := &TableMeta{
		columns:   columnMetas,
		tableName: internal.UnderscoreName(v.Name()),
		typ:       rtype,
		fieldMap:  fieldMap,
	}
	for _, o := range opts {
		o(TableMeta)
	}
	t.metas.Store(rtype, TableMeta)
	return TableMeta, nil

}
