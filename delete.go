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
	"database/sql"

	"github.com/valyala/bytebufferpool"
)

// Deleter builds DELETE query
type Deleter[T any] struct {
	builder
	session
	table interface{}
	where []Predicate
}

// NewDeleter 开始构建一个 DELETE 查询
func NewDeleter[T any](sess session) *Deleter[T] {
	return &Deleter[T]{
		builder: builder{
			core:   sess.getCore(),
			buffer: bytebufferpool.Get(),
		},
		session: sess,
	}
}

// Build returns DELETE query
func (d *Deleter[T]) Build() (*Query, error) {
  defer bytebufferpool.Put(d.buffer)
	_, _ = d.buffer.WriteString("DELETE FROM ")
	var err error
	if d.table == nil {
		d.table = new(T)
	}
	d.meta, err = d.metaRegistry.Get(d.table)
	if err != nil {
		return nil, err
	}

	d.quote(d.meta.TableName)
	if len(d.where) > 0 {
		d.writeString(" WHERE ")
		err = d.buildPredicates(d.where)
		if err != nil {
			return nil, err
		}
	}
	d.end()
	return &Query{SQL: d.buffer.String(), Args: d.args}, nil
}

// From accepts model definition
func (d *Deleter[T]) From(table interface{}) *Deleter[T] {
	d.table = table
	return d
}

// Where accepts predicates
func (d *Deleter[T]) Where(predicates ...Predicate) *Deleter[T] {
	d.where = predicates
	return d
}

func (d *Deleter[T]) Exec(ctx context.Context) (sql.Result, error) {
	query, err := d.Build()
	if err != nil {
		return nil, err
	}
	return newQuerier[T](d.session, query).Exec(ctx)
}
