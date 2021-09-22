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
	// key 是字段名
	fieldMap map[string]*ColumnMeta
	typ      reflect.Type
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
	// 这里传过来的 table 应该是结构体指针，例如 *User
	Get(table interface{}, opts ...tableMetaOption) (*TableMeta, error)
	Register(table interface{}, opts ...tableMetaOption) (*TableMeta, error)
}

var defaultMetaRegistry = &tagMetaRegistry{}

// 我们的默认实现，它使用如下的规则
// 1. 结构体名字转表名，按照驼峰转下划线的方式
// 2. 字段名转列名，也是按照驼峰转下划线的方式
type tagMetaRegistry struct {
	metas      sync.Map
	metasMutex sync.Mutex
}

func (t *tagMetaRegistry) Get(table interface{}) (*TableMeta, error) {
	// 从 metas 里面去取，没有的话就调用 Register 新注册一个
	if v, ok := t.metas.Load(reflect.TypeOf(table)); ok {
		return v.(*TableMeta), nil
	}
	t.metasMutex.Lock()
	defer t.metasMutex.Unlock()
	tableMeta, err := t.Register(table, func(meta *TableMeta) {
		meta.tableName = internal.TableName(table)
		meta.typ = reflect.TypeOf(table)
	})
	return tableMeta, err
}

func (t *tagMetaRegistry) Register(table interface{}, opts ...tableMetaOption) (*TableMeta, error) {
	// 拿到 table 的反射
	// 遍历 table 的所有字段
	// 对每一个字段检查 tag eql, 如果 eql 里面有 auto_increment, primary_key，就相应设置 ColumnMeta的值
	// 最后处理所有的 opts
	// 塞到 map 里面
	ColumnMeta := &ColumnMeta{}
	v := reflect.ValueOf(table).Elem()
	for i := 0; i < v.NumField(); i++ {
		structField := v.Type().Field(i)
		tag := structField.Tag.Get("eql")
		isAuto := strings.Contains(tag, "auto_increment")
		isKey := strings.Contains(tag, "primary_key")
		ColumnMeta.columnName = structField.Name
		ColumnMeta.fieldName = internal.UnderscoreName(structField.Name)
		ColumnMeta.typ = structField.Type
		if isAuto {
			ColumnMeta.isAutoIncrement = true
		}
		if isKey {
			ColumnMeta.isPrimaryKey = true
		}
	}
	TableMeta := &TableMeta{}
	for _, o := range opts {
		o(TableMeta)
	}
	TableMeta.columns = append(TableMeta.columns, ColumnMeta)
	TableMeta.tableName = internal.TableName(table)
	TableMeta.typ = reflect.TypeOf(table)
	TableMeta.fieldMap[ColumnMeta.fieldName] = ColumnMeta
	return TableMeta, nil

}
