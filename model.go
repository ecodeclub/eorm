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
	columnMeta := []*ColumnMeta{}
	fieldMap := make(map[string]*ColumnMeta)
	for i := 0; i < v.NumField(); i++ {
		dataMeta := &ColumnMeta{}
		structField := v.Field(i)
		tag := structField.Tag.Get("eql")
		isAuto := strings.Contains(tag, "auto_increment")
		isKey := strings.Contains(tag, "primary_key")
		dataMeta.columnName = internal.UnderscoreName(structField.Name)
		dataMeta.fieldName = structField.Name
		dataMeta.typ = structField.Type
		dataMeta.isAutoIncrement = isAuto
		dataMeta.isPrimaryKey = isKey
		columnMeta = append(columnMeta, dataMeta)
		fieldMap[dataMeta.fieldName] = dataMeta
	}
	TableMeta := &TableMeta{}
	TableMeta.columns = columnMeta
	TableMeta.tableName = internal.UnderscoreName(v.Name())
	TableMeta.typ = rtype
	TableMeta.fieldMap = fieldMap
	for _, o := range opts {
		o(TableMeta)
	}
	t.metas.Store(reflect.TypeOf(table), TableMeta)
	return TableMeta, nil

}
