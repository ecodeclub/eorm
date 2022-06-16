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
	"errors"
	"github.com/gotomicro/eorm/internal/errs"
	"github.com/gotomicro/eorm/internal/model"
	"github.com/valyala/bytebufferpool"
)

// Inserter is used to construct an insert query
// More details check Build function
type Inserter[T any] struct {
	builder
	session
	columns []string
	values  []*T
}

// NewInserter 开始构建一个 INSERT 查询
func NewInserter[T any](sess session) *Inserter[T] {
	return &Inserter[T]{
		builder: builder{
			core: sess.getCore(),
			buffer: bytebufferpool.Get(),
		},
		session: sess,
	}
}

// Build function build the query
// notes:
// - All the values from function Values should have the same type.
// - It will insert all columns including auto-increment primary key
func (i *Inserter[T]) Build() (*Query, error) {
	var err error
	if len(i.values) == 0 {
		return &Query{}, errors.New("插入0行")
	}
	i.writeString("INSERT INTO ")
	i.meta, err = i.metaRegistry.Get(i.values[0])
	if err != nil {
		return &Query{}, err
	}
	i.quote(i.meta.TableName)
	i.writeString("(")
	fields, err := i.buildColumns()
	if err != nil {
		return nil, err
	}
	i.writeString(")")
	i.writeString(" VALUES")
	for index, val := range i.values {
		if index > 0 {
			i.comma()
		}
		i.writeString("(")
		refVal := i.valCreator(val, i.meta)
		for j, v := range fields {
			fdVal, err := refVal.Field(v.FieldName)
			if err != nil {
				return nil, err
			}
			i.parameter(fdVal)
			if j != len(fields)-1 {
				i.comma()
			}
		}
		i.writeString(")")
	}
	i.end()
	return &Query{SQL: i.buffer.String(), Args: i.args}, nil
}

// Columns specifies the columns that need to be inserted
// if cs is empty, all columns will be inserted
// cs must be the same with the field name in model
func (i *Inserter[T]) Columns(cs ...string) *Inserter[T] {
	i.columns = cs
	return i
}

// Values specify the rows
// all the elements must be the same type
// and users are supposed to passing at least one element
func (i *Inserter[T]) Values(values ...*T) *Inserter[T]{
	i.values = values
	return i
}

// Exec 发起查询
func (i *Inserter[T]) Exec(ctx context.Context) (sql.Result, error){
	query, err := i.Build()
	if err != nil {
		return nil, err
	}
	return newQuerier[T](i.session, query).Exec(ctx)
}

func (i *Inserter[T]) buildColumns() ([]*model.ColumnMeta, error) {
	cs := i.meta.Columns
	if len(i.columns) != 0 {
		cs = make([]*model.ColumnMeta, 0, len(i.columns))
		for index, c := range i.columns {
			v, isOk := i.meta.FieldMap[c]
			if !isOk {
				return cs, errs.NewInvalidFieldError(c)
			}
			i.quote(v.ColumnName)
			if index != len(i.columns)-1 {
				i.comma()
			}
			cs = append(cs, v)
		}
	} else {
		for index, val := range i.meta.Columns {
			i.quote(val.ColumnName)
			if index != len(cs)-1 {
				i.comma()
			}
		}
	}
	return cs, nil
}
