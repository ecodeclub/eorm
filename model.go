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

// MetadaRegistry stores table metadata
type MetaRegistry interface {
	Get(table interface{}) (*TableMeta, error)
	Register(table interface{}, opts ...tableMetaOption) (*TableMeta, error)
}

var defaultMetaRegistry = &tagMetaRegistry{}

// tagMetaRegistry is the default implementation based on tag eql
type tagMetaRegistry struct {
	metas sync.Map
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
	tableMeta := &TableMeta{
		columns:   columnMetas,
		tableName: internal.UnderscoreName(v.Name()),
		typ:       rtype,
		fieldMap:  fieldMap,
	}
	for _, o := range opts {
		o(tableMeta)
	}
	t.metas.Store(rtype, tableMeta)
	return tableMeta, nil

}
