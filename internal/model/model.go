// Copyright 2021 gotomicro
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strings"
	"sync"

	// nolint
	"unicode"
)

var (
	scannerType      = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	driverValuerType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
)

// TableMeta represents data model, or a table
type TableMeta struct {
	TableName string
	Columns   []*ColumnMeta
	// FieldMap 是字段名到列元数据的映射
	FieldMap map[string]*ColumnMeta
	// ColumnMap 是列名到列元数据的映射
	ColumnMap map[string]*ColumnMeta
	Typ       reflect.Type
}

// ColumnMeta represents model's field, or column
type ColumnMeta struct {
	ColumnName      string
	FieldName       string
	Typ             reflect.Type
	IsPrimaryKey    bool
	IsAutoIncrement bool
	// Offset 是字段偏移量。需要注意的是，这里的字段偏移量是相对于整个结构体的偏移量
	// 例如在组合的情况下，
	// type A struct {
	//     name string
	//     B
	// }
	// type B struct {
	//     age int
	// }
	// age 的偏移量是相对于 A 的起始地址的偏移量
	Offset uintptr
	// IsHolderType 用于表达是否是 Holder 的类型
	// 所谓的 Holder，就是指同时实现了 sql.Scanner 和 driver.Valuer 两个接口的类型
	IsHolderType bool
	// FieldIndexes 用于表达从最外层结构体找到当前ColumnMeta对应的Field所需要的索引集
	FieldIndexes []int
}

// TableMetaOption represents options of TableMeta, this options will cover default cover.
type TableMetaOption func(meta *TableMeta)

// MetaRegistry stores table metadata
type MetaRegistry interface {
	Get(table interface{}) (*TableMeta, error)
	Register(table interface{}, opts ...TableMetaOption) (*TableMeta, error)
}

func NewMetaRegistry() MetaRegistry {
	return &tagMetaRegistry{}
}

// tagMetaRegistry is the default implementation based on tag eorm
type tagMetaRegistry struct {
	metas sync.Map
}

func NewTagMetaRegistry() MetaRegistry {
	return &tagMetaRegistry{}
}

// Get the metadata for each column of the data table,
// If there is none, it will register one and return the metadata for each column
func (t *tagMetaRegistry) Get(table interface{}) (*TableMeta, error) {
	if v, ok := t.metas.Load(reflect.TypeOf(table)); ok {
		return v.(*TableMeta), nil
	}
	return t.Register(table)
}

// Register function generates a metadata for each column and places it in a thread-safe mapping to facilitate direct access to the metadata.
// And the metadata can be modified by user-defined methods opts
func (t *tagMetaRegistry) Register(table interface{}, opts ...TableMetaOption) (*TableMeta, error) {
	rtype := reflect.TypeOf(table)
	v := rtype.Elem()
	var columnMetas []*ColumnMeta
	lens := v.NumField()
	fieldMap := make(map[string]*ColumnMeta, lens)
	columnMap := make(map[string]*ColumnMeta, lens)
	for i := 0; i < lens; i++ {
		structField := v.Field(i)
		tag := structField.Tag.Get("eorm")
		var isKey, isAuto, isIgnore bool
		for _, t := range strings.Split(tag, ",") {
			switch t {
			case "primary_key":
				isKey = true
			case "auto_increment":
				isAuto = true
			case "-":
				isIgnore = true
			}
		}
		if isIgnore {
			// skip the field.
			continue
		}
		columnMeta := &ColumnMeta{
			ColumnName:      underscoreName(structField.Name),
			FieldName:       structField.Name,
			Typ:             structField.Type,
			IsAutoIncrement: isAuto,
			IsPrimaryKey:    isKey,
			Offset:          structField.Offset,
			IsHolderType:    structField.Type.AssignableTo(scannerType) && structField.Type.AssignableTo(driverValuerType),
			FieldIndexes:    []int{i},
		}
		columnMetas = append(columnMetas, columnMeta)
		fieldMap[columnMeta.FieldName] = columnMeta
		columnMap[columnMeta.ColumnName] = columnMeta
	}
	tableMeta := &TableMeta{
		Columns:   columnMetas,
		TableName: underscoreName(v.Name()),
		Typ:       rtype,
		FieldMap:  fieldMap,
		ColumnMap: columnMap,
	}
	for _, o := range opts {
		o(tableMeta)
	}
	t.metas.Store(rtype, tableMeta)
	return tableMeta, nil
}

// IgnoreFieldsOption function provide an option to ignore some fields when register table.
func IgnoreFieldsOption(fieldNames ...string) TableMetaOption {
	return func(meta *TableMeta) {
		for _, field := range fieldNames {
			// has field in the TableMeta
			if _, ok := meta.FieldMap[field]; ok {
				// delete field in columns slice
				for index, column := range meta.Columns {
					if column.FieldName == field {
						meta.Columns = append(meta.Columns[:index], meta.Columns[index+1:]...)
						break
					}
				}
				// delete field in fieldMap
				delete(meta.FieldMap, field)
			}
		}
	}
}

// underscoreName function mainly converts upper case to lower case and adds an underscore in between
func underscoreName(tableName string) string {
	var buf []byte
	for i, v := range tableName {
		if unicode.IsUpper(v) {
			if i != 0 {
				buf = append(buf, '_')
			}
			buf = append(buf, byte(unicode.ToLower(v)))
		} else {
			buf = append(buf, byte(v))
		}

	}
	return string(buf)
}
