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
	"fmt"
	"reflect"

	"github.com/gotomicro/eql/internal"
	"github.com/valyala/bytebufferpool"
)

// Updater is the builder responsible for building UPDATE query
type Updater struct {
	builder
	table          interface{}
	tableEle       reflect.Value
	where          []Predicate
	assigns        []Assignable
	nullAssertFunc NullAssertFunc
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

	u.buffer.WriteString("UPDATE ")
	u.quote(u.meta.tableName)
	u.buffer.WriteString(" SET ")
	if len(u.assigns) == 0 {
		err = u.buildDefaultColumns()
	} else {
		err = u.buildAssigns()
	}
	if err != nil {
		return nil, err
	}

	// TODO WHERE

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
			set, err := u.buildColumn(a.name)
			if err != nil {
				return err
			}
			has = has || set
		case columns:
			for _, c := range a.cs {
				if has {
					u.comma()
				}
				set, err := u.buildColumn(c)
				if err != nil {
					return err
				}
				has = has || set
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
		return errors.New("eql: value unset")
	}
	return nil
}

func (u *Updater) buildColumn(field string) (bool, error) {
	c, ok := u.meta.fieldMap[field]
	if !ok {
		return false, internal.NewInvalidColumnError(field)
	}
	return u.setColumn(c), nil
}

func (u *Updater) setColumn(c *ColumnMeta) bool {
	val := u.tableEle.FieldByName(c.fieldName).Interface()
	isNull := u.nullAssertFunc(val)
	if !isNull {
		u.quote(c.columnName)
		u.buffer.WriteByte('=')
		u.parameter(val)
		return true
	}
	return false
}

func (u *Updater) buildDefaultColumns() error {
	has := false
	for _, c := range u.meta.columns {
		if has {
			u.buffer.WriteByte(',')
		}
		val := u.tableEle.FieldByName(c.fieldName).Interface()
		isNull := u.nullAssertFunc(val)
		if !isNull {
			u.quote(c.columnName)
			u.buffer.WriteByte('=')
			u.parameter(val)
			has = true
		}
	}
	if !has {
		return errors.New("value unset")
	}
	return nil
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
