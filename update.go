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

package eorm

import (
	"context"
	"fmt"
	"reflect"

	"github.com/gotomicro/eorm/internal/errs"
	"github.com/gotomicro/eorm/internal/valuer"

	"github.com/valyala/bytebufferpool"
)

// Updater is the builder responsible for building UPDATE query
type Updater[T any] struct {
	builder
	session
	table   interface{}
	val     valuer.Value
	where   []Predicate
	assigns []Assignable
}

// NewUpdater 开始构建一个 UPDATE 查询
func NewUpdater[T any](sess session) *Updater[T] {
	return &Updater[T]{
		builder: builder{
			core:   sess.getCore(),
			buffer: bytebufferpool.Get(),
		},
		session: sess,
	}
}

func (u *Updater[T]) Update(val *T) *Updater[T] {
	u.table = val
	return u
}

// Build returns UPDATE query
func (u *Updater[T]) Build() (*Query, error) {
	defer bytebufferpool.Put(u.buffer)
	var err error
	t := new(T)
	if u.table == nil {
		u.table = t
	}
	u.meta, err = u.metaRegistry.Get(t)
	if err != nil {
		return nil, err
	}

	u.val = u.valCreator.NewBasicTypeValue(u.table, u.meta)
	u.args = make([]interface{}, 0, len(u.meta.Columns))

	u.writeString("UPDATE ")
	u.quote(u.meta.TableName)
	u.writeString(" SET ")
	if len(u.assigns) == 0 {
		err = u.buildDefaultColumns()
	} else {
		err = u.buildAssigns()
	}
	if err != nil {
		return nil, err
	}

	if len(u.where) > 0 {
		u.writeString(" WHERE ")
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

func (u *Updater[T]) buildAssigns() error {
	has := false
	for _, assign := range u.assigns {
		if has {
			u.comma()
		}
		switch a := assign.(type) {
		case Column:
			c, ok := u.meta.FieldMap[a.name]
			if !ok {
				return errs.NewInvalidFieldError(a.name)
			}
			val, _ := u.val.Field(a.name)
			u.quote(c.ColumnName)
			_ = u.buffer.WriteByte('=')
			u.parameter(val)
			has = true
		case columns:
			for _, name := range a.cs {
				c, ok := u.meta.FieldMap[name]
				if !ok {
					return errs.NewInvalidFieldError(name)
				}
				val, _ := u.val.Field(name)
				if has {
					u.comma()
				}
				u.quote(c.ColumnName)
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
			return fmt.Errorf("eorm: unsupported assignment %v", a)
		}
	}
	if !has {
		return errs.NewValueNotSetError()
	}
	return nil
}

func (u *Updater[T]) buildDefaultColumns() error {
	has := false
	for _, c := range u.meta.Columns {
		val, _ := u.val.Field(c.FieldName)
		if has {
			_ = u.buffer.WriteByte(',')
		}
		u.quote(c.ColumnName)
		_ = u.buffer.WriteByte('=')
		u.parameter(val)
		has = true
	}
	if !has {
		return errs.NewValueNotSetError()
	}
	return nil
}

// Set represents SET clause
func (u *Updater[T]) Set(assigns ...Assignable) *Updater[T] {
	u.assigns = assigns
	return u
}

// Where represents WHERE clause
func (u *Updater[T]) Where(predicates ...Predicate) *Updater[T] {
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
	numField := val.NumField()

	fdTypes := make([]reflect.StructField, 0, numField)
	fdValues := make([]reflect.Value, 0, numField)
	flapFields(entity, &fdTypes, &fdValues)

	res := make([]Assignable, 0, len(fdTypes))
	for i := 0; i < len(fdTypes); i++ {
		fieldVal := fdValues[i]
		fieldTyp := fdTypes[i]
		if filter(fieldTyp, fieldVal) {
			res = append(res, Assign(fieldTyp.Name, fieldVal.Interface()))
		}
	}
	return res
}

func flapFields(entity interface{}, fdTypes *[]reflect.StructField, fdValues *[]reflect.Value) {
	typ := reflect.TypeOf(entity)
	val := reflect.ValueOf(entity)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
		val = val.Elem()
	}
	numField := val.NumField()
	for i := 0; i < numField; i++ {
		if !typ.Field(i).Anonymous {
			*fdTypes = append(*fdTypes, typ.Field(i))
			*fdValues = append(*fdValues, val.Field(i))
			continue
		}
		flapFields(val.Field(i).Interface(), fdTypes, fdValues)
	}
}

// Exec sql
func (u *Updater[T]) Exec(ctx context.Context) Result {
	query, err := u.Build()
	if err != nil {
		return Result{err: err}
	}
	qc := &QueryContext{
		Builder: u,
		Type:    "Update",
		meta:    u.meta,
		q:       query,
	}
	// 这里可把 s.meta， query 从传参去掉了 然后在正式代码里 exec 将改为 Exec
	return newQuerier[T](u.session, query, u.meta).exec(ctx, qc)
	// return newQuerier[T](u.session, query, u.meta).Exec(ctx)
}
