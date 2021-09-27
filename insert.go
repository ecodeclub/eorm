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
	"errors"
	"reflect"
	"strings"
)

// Inserter is used to construct an insert query
type Inserter struct {
	tableName   string
	fields      []string
	fieldMap    map[string]*ColumnMeta
	fieldsParam []string
	valuesParam []interface{}
}

func (i *Inserter) Build() (*Query, error) {

	if len(i.valuesParam) == 0 {
		return &Query{}, errors.New("no values")
	}
	builder := strings.Builder{}
	builder.WriteString("INSERT INTO")
	meta, _ := defaultMetaRegistry.Get(i.valuesParam[0])
	i.tableName = meta.tableName
	builder.WriteString(" `" + i.tableName + "`")
	builder.WriteString("(")
	if i.fieldsParam != nil {
		for index, v := range i.fieldsParam {
			builder.WriteString("`")
			builder.WriteString(v)
			builder.WriteString("`")
			if index != len(i.fieldsParam)-1 {
				builder.WriteString(",")
			}
		}
	} else {
		for index, v := range meta.columns {
			builder.WriteString("`")
			builder.WriteString(v.columnName)
			builder.WriteString("`")
			if index != len(meta.columns)-1 {
				builder.WriteString(",")
			}
		}
	}
	builder.WriteString(")")
	builder.WriteString(" VALUES")
	builder.WriteString("(")
	args := make([]interface{}, 0, len(meta.columns))
	for index, value := range i.valuesParam {
		meta, _ := defaultMetaRegistry.Get(value)
		if i.tableName != meta.tableName {
			return &Query{}, errors.New("multiple values of different type")
		}
		if i.fieldsParam != nil {
			refVal := reflect.ValueOf(value).Elem()
			for index, value := range i.fieldsParam {
				for _, v := range meta.columns {
					if value == v.columnName {
						field := refVal.FieldByName(v.fieldName)
						val := field.Interface()
						args = append(args, val)
						builder.WriteString("?")
						if index != len(i.fieldsParam)-1 {
							builder.WriteString(",")
						}
					}
				}
			}
			if index != len(i.valuesParam)-1 {
				builder.WriteString("),(")
			}
		} else {
			refVal := reflect.ValueOf(value).Elem()
			for index, c := range meta.columns {
				field := refVal.FieldByName(c.fieldName)
				val := field.Interface()
				args = append(args, val)
				builder.WriteString("?")
				if index != len(meta.columns)-1 {
					builder.WriteString(",")
				}
			}
			if index != len(i.valuesParam)-1 {
				builder.WriteString("),(")
			}
		}
	}
	builder.WriteString(");")
	return &Query{SQL: builder.String(), Args: args}, nil
}

// Columns specifies the columns that need to be inserted
// if cs is empty, all columns will be inserted except auto increment columns
func (i *Inserter) Columns(cs ...string) *Inserter {
	i.fieldsParam = cs
	return i
}

// Values specify the rows
// all the elements must be the same structure
func (i *Inserter) Values(values ...interface{}) *Inserter {
	i.valuesParam = values
	return i
}

// OnDuplicateKey generate MysqlUpserter
// if the dialect is not MySQL, it will panic
func (i *Inserter) OnDuplicateKey() *MysqlUpserter {

	panic("implement me")
}

// OnConflict generate PgSQLUpserter
// if the dialect is not PgSQL, it will panic
func (i *Inserter) OnConflict(cs ...string) *PgSQLUpserter {

	panic("implement me")
}
