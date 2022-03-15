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
	"fmt"
	"reflect"

	"github.com/gotomicro/eql/internal"
	"github.com/valyala/bytebufferpool"
)

// Updater is the builder responsible for building UPDATE query
type Updater struct {
	builder
	table    interface{}
	tableEle reflect.Value
	where    []Predicate
	assigns  []Assignable
}

// Build returns UPDATE query
func (u *Updater) Build() (*Query, error) {
	defer bytebufferpool.Put(u.buffer)
	var err error
	u.meta, err = u.registry.Get(u.table)
	if err != nil {
		return nil, err
	}

	u.tableEle = reflect.ValueOf(u.table).Elem()
	u.args = make([]interface{}, 0, len(u.meta.columns))

	_, _ = u.buffer.WriteString("UPDATE ")
	u.quote(u.meta.tableName)
	_, _ = u.buffer.WriteString(" SET ")
	if len(u.assigns) == 0 {
		err = u.buildDefaultColumns()
	} else {
		err = u.buildAssigns()
	}
	if err != nil {
		return nil, err
	}

	if len(u.where) > 0 {
		_, _ = u.buffer.WriteString(" WHERE ")
		err = u.buildPredicates(u.where)
		if err != nil {
			return nil, err
		}
	}

	u.end()
	return &Query{
		SQL:  u.buffer.String(),
		Args: u.args,
	}, nil
}

func (u *Updater) buildAssigns() error {
	has := false
	for _, assign := range u.assigns {
		if has {
			u.comma()
		}
		switch a := assign.(type) {
		case Column:
			c, ok := u.meta.fieldMap[a.name]
			if !ok {
				return internal.NewInvalidColumnError(a.name)
			}
			val := u.getValue(a.name)
			u.quote(c.columnName)
			_ = u.buffer.WriteByte('=')
			u.parameter(val)
			has = true
		case columns:
			for _, name := range a.cs {
				c, ok := u.meta.fieldMap[name]
				if !ok {
					return internal.NewInvalidColumnError(name)
				}
				val := u.getValue(name)
				if has {
					u.comma()
				}
				u.quote(c.columnName)
				_ = u.buffer.WriteByte('=')
				u.parameter(val)
				has = true
			}
		case Assignment:
			if err := u.buildExpr(binaryExpr(a)); err != nil {
				return err
			}
			has = true
		default:
			return fmt.Errorf("eql: unsupported assignment %v", a)
		}
	}
	if !has {
		return internal.NewValueNotSetError()
	}
	return nil
}

func (u *Updater) buildDefaultColumns() error {
	has := false
	for _, c := range u.meta.columns {
		val := u.getValue(c.fieldName)
		if has {
			_ = u.buffer.WriteByte(',')
		}
		u.quote(c.columnName)
		_ = u.buffer.WriteByte('=')
		u.parameter(val)
		has = true
	}
	if !has {
		return internal.NewValueNotSetError()
	}
	return nil
}

func (u *Updater) getValue(fieldName string) interface{} {
	val := u.tableEle.FieldByName(fieldName)
	return val.Interface()
}

// Set represents SET clause
func (u *Updater) Set(assigns ...Assignable) *Updater {
	u.assigns = assigns
	return u
}

// Where represents WHERE clause
func (u *Updater) Where(predicates ...Predicate) *Updater {
	u.where = predicates
	return u
}

// AssignNotNilColumns uses the non-nil value to construct the Assignable instances.
func AssignNotNilColumns(entity interface{}) []Assignable {
	return AssignColumns(entity, func(typ reflect.StructField, val reflect.Value) bool {
		switch val.Kind() {
		case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.UnsafePointer, reflect.Interface, reflect.Slice:
			return !val.IsNil()
		}
		return true
	})
}

// AssignNotZeroColumns uses the non-zero value to construct the Assignable instances.
func AssignNotZeroColumns(entity interface{}) []Assignable {
	return AssignColumns(entity, func(typ reflect.StructField, val reflect.Value) bool {
		return !val.IsZero()
	})
}

// AssignColumns will check all columns and then apply the filter function.
// If the returned value is true, this column will be updated.
func AssignColumns(entity interface{}, filter func(typ reflect.StructField, val reflect.Value) bool) []Assignable {
	val := reflect.ValueOf(entity).Elem()
	typ := reflect.TypeOf(entity).Elem()
	numField := val.NumField()
	res := make([]Assignable, 0, numField)
	for i := 0; i < numField; i++ {
		fieldVal := val.Field(i)
		fieldTyp := typ.Field(i)
		if filter(fieldTyp, fieldVal) {
			res = append(res, Assign(fieldTyp.Name, fieldVal.Interface()))
		}
	}
	return res
}
