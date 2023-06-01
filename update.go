// Copyright 2021 ecodeclub
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
	"reflect"

	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/valyala/bytebufferpool"
)

var _ QueryBuilder = &Updater[any]{}

// Updater is the builder responsible for building UPDATE query
type Updater[T any] struct {
	Session
	updaterBuilder
	table interface{}
}

// NewUpdater 开始构建一个 UPDATE 查询
func NewUpdater[T any](sess Session) *Updater[T] {
	return &Updater[T]{
		updaterBuilder: updaterBuilder{
			builder: builder{
				core:   sess.getCore(),
				buffer: bytebufferpool.Get(),
			},
		},
		Session: sess,
	}
}

func (u *Updater[T]) Update(val *T) *Updater[T] {
	u.table = val
	return u
}

// Build returns UPDATE query
func (u *Updater[T]) Build() (Query, error) {
	defer bytebufferpool.Put(u.buffer)
	var err error
	t := new(T)
	if u.table == nil {
		u.table = t
	}
	u.meta, err = u.metaRegistry.Get(t)
	if err != nil {
		return EmptyQuery, err
	}

	u.val = u.valCreator.NewPrimitiveValue(u.table, u.meta)
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
		return EmptyQuery, err
	}

	if len(u.where) > 0 {
		u.writeString(" WHERE ")
		err = u.buildPredicates(u.where)
		if err != nil {
			return EmptyQuery, err
		}
	}

	u.end()
	return Query{
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
			refVal, _ := u.val.Field(a.name)
			u.quote(c.ColumnName)
			_ = u.buffer.WriteByte('=')
			u.parameter(refVal.Interface())
			has = true
		case columns:
			for _, name := range a.cs {
				c, ok := u.meta.FieldMap[name]
				if !ok {
					return errs.NewInvalidFieldError(name)
				}
				refVal, _ := u.val.Field(name)
				if has {
					u.comma()
				}
				u.quote(c.ColumnName)
				_ = u.buffer.WriteByte('=')
				u.parameter(refVal.Interface())
				has = true
			}
		case Assignment:
			if err := u.buildExpr(binaryExpr(a)); err != nil {
				return err
			}
			has = true
		default:
			return errs.ErrUnsupportedAssignment
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
		refVal, _ := u.val.Field(c.FieldName)
		if u.ignoreZeroVal && isZeroValue(refVal) {
			continue
		}
		if u.ignoreNilVal && isNilValue(refVal) {
			continue
		}
		if has {
			_ = u.buffer.WriteByte(',')
		}
		u.quote(c.ColumnName)
		_ = u.buffer.WriteByte('=')
		u.parameter(refVal.Interface())
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

// SkipNilValue 忽略 nil 值 columns
func (u *Updater[T]) SkipNilValue() *Updater[T] {
	u.ignoreNilVal = true
	return u
}

func isNilValue(val reflect.Value) bool {
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return val.IsNil()
	}
	return false
}

// SkipZeroValue 忽略零值 columns
func (u *Updater[T]) SkipZeroValue() *Updater[T] {
	u.ignoreZeroVal = true
	return u
}

func isZeroValue(val reflect.Value) bool {
	return val.IsZero()
}

// Exec sql
func (u *Updater[T]) Exec(ctx context.Context) Result {
	query, err := u.Build()
	if err != nil {
		return Result{err: err}
	}
	return newQuerier[T](u.Session, query, u.meta, UPDATE).Exec(ctx)
}
