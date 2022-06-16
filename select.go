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
	"github.com/gotomicro/eorm/internal/errs"
	"github.com/valyala/bytebufferpool"
)

// Selector represents a select query
type Selector[T any] struct {
	builder
	session
	columns  []Selectable
	table    interface{}
	where    []Predicate
	distinct bool
	having   []Predicate
	groupBy  []string
	orderBy  []OrderBy
	offset   int
	limit    int
}

// NewSelector 创建一个 Selector
func NewSelector[T any](sess session) *Selector[T] {
	return &Selector[T]{
		builder: builder{
			core: sess.getCore(),
			buffer: bytebufferpool.Get(),
		},
		session: sess,
	}
}

// Build returns Select Query
func (s *Selector[T]) Build() (*Query, error) {
	defer bytebufferpool.Put(s.buffer)
	var err error
	s.meta, err = s.metaRegistry.Get(s.table)
	if err != nil {
		return nil, err
	}
	s.writeString("SELECT ")
	if len(s.columns) == 0 {
		s.buildAllColumns()
	} else {
		err = s.buildSelectedList()
		if err != nil {
			return nil, err
		}
	}
	s.writeString(" FROM ")
	s.quote(s.meta.TableName)
	if len(s.where) > 0 {
		s.writeString(" WHERE ")
		err = s.buildPredicates(s.where)
		if err != nil {
			return nil, err
		}
	}

	// group by
	if len(s.groupBy) > 0 {
		err = s.buildGroupBy()
		if err != nil {
			return nil, err
		}
	}

	// order by
	if len(s.orderBy) > 0 {
		err = s.buildOrderBy()
		if err != nil {
			return nil, err
		}
	}

	// having
	if len(s.having) > 0 {
		s.writeString(" HAVING ")
		err = s.buildPredicates(s.having)
		if err != nil {
			return nil, err
		}
	}

	if s.offset > 0 {
		s.writeString(" OFFSET ")
		s.parameter(s.offset)
	}

	if s.limit > 0 {
		s.writeString(" LIMIT ")
		s.parameter(s.limit)
	}
	s.end()
	return &Query{SQL: s.buffer.String(), Args: s.args}, nil
}

func (s *Selector[T]) buildOrderBy() error {
	s.writeString(" ORDER BY ")
	for i, ob := range s.orderBy {
		if i > 0 {
			s.comma()
		}
		for _, c := range ob.fields {
			cMeta, ok := s.meta.FieldMap[c]
			if !ok {
				return errs.NewInvalidFieldError(c)
			}
			s.quote(cMeta.ColumnName)
		}
		s.space()
		s.writeString(ob.order)
	}
	return nil
}

func (s *Selector[T]) buildGroupBy() error {
	s.writeString(" GROUP BY ")
	for i, gb := range s.groupBy {
		cMeta, ok := s.meta.FieldMap[gb]
		if !ok {
			return errs.NewInvalidFieldError(gb)
		}
		if i > 0 {
			s.comma()
		}
		s.quote(cMeta.ColumnName)
	}
	return nil
}

func (s *Selector[T]) buildAllColumns() {
	for i, cMeta := range s.meta.Columns {
		if i > 0 {
			s.comma()
		}
		// it should never return error, we can safely ignore it
		_ = s.buildColumn(cMeta.FieldName, "")
	}
}

// buildSelectedList users specify columns
func (s *Selector[T]) buildSelectedList() error {
	s.aliases = make(map[string]struct{})
	for i, selectable := range s.columns {
		if i > 0 {
			s.comma()
		}
		switch expr := selectable.(type) {
		case Column:
			err := s.buildColumn(expr.name, expr.alias)
			if err != nil {
				return err
			}
		case columns:
			for j, c := range expr.cs {
				if j > 0 {
					s.comma()
				}
				err := s.buildColumn(c, "")
				if err != nil {
					return err
				}
			}
		case Aggregate:
			if err := s.selectAggregate(expr); err != nil {
				return err
			}
		case RawExpr:
			s.buildRawExpr(expr)
		}
	}
	return nil

}
func (s *Selector[T]) selectAggregate(aggregate Aggregate) error {
	s.writeString(aggregate.fn)
	s.writeByte('(')
	cMeta, ok := s.meta.FieldMap[aggregate.arg]
	s.aliases[aggregate.alias] = struct{}{}
	if !ok {
		return errs.NewInvalidFieldError(aggregate.arg)
	}
	s.quote(cMeta.ColumnName)
	s.writeByte(')')
	if aggregate.alias != "" {
		if _, ok := s.aliases[aggregate.alias]; ok {
			s.writeString(" AS ")
			s.quote(aggregate.alias)
		}
	}
	return nil
}

func (s *Selector[T]) buildColumn(field, alias string) error {
	cMeta, ok := s.meta.FieldMap[field]
	if !ok {
		return errs.NewInvalidFieldError(field)
	}
	s.quote(cMeta.ColumnName)
	if alias != "" {
		s.aliases[alias] = struct{}{}
		s.writeString(" AS ")
		s.quote(alias)
	}
	return nil
}

// Select 指定查询的列。
// 列可以是物理列，也可以是聚合函数，或者 RawExpr
func (s *Selector[T]) Select(columns ...Selectable) *Selector[T] {
	s.columns = columns
	return s
}

// From specifies the table which must be pointer of structure
func (s *Selector[T]) From(table interface{}) *Selector[T] {
	s.table = table
	return s
}

// Where accepts predicates
func (s *Selector[T]) Where(predicates ...Predicate) *Selector[T] {
	s.where = predicates
	return s
}

// Distinct indicates using keyword DISTINCT
func (s *Selector[T]) Distinct() *Selector[T] {
	s.distinct = true
	return s
}

// Having accepts predicates
func (s *Selector[T]) Having(predicates ...Predicate) *Selector[T] {
	s.having = predicates
	return s
}

// GroupBy means "GROUP BY"
func (s *Selector[T]) GroupBy(columns ...string) *Selector[T] {
	s.groupBy = columns
	return s
}

// OrderBy means "ORDER BY"
func (s *Selector[T]) OrderBy(orderBys ...OrderBy) *Selector[T] {
	s.orderBy = orderBys
	return s
}

// Limit limits the size of result set
func (s *Selector[T]) Limit(limit int) *Selector[T] {
	s.limit = limit
	return s
}

// Offset was used by "LIMIT"
func (s *Selector[T]) Offset(offset int) *Selector[T] {
	s.offset = offset
	return s
}

// Get 方法会执行查询，并且返回一条数据
// 注意，在不同的数据库情况下，第一条数据可能是按照不同的列来排序的
// 而且要注意，这个方法会强制设置 Limit 1
func (s *Selector[T]) Get(ctx context.Context) (*T, error) {
	query, err := s.Limit(1).Build()
	if err != nil {
		return nil, err
	}
	return newQuerier[T](s.session, query).Get(ctx)
}


// OrderBy specify fields and ASC
type OrderBy struct {
	fields []string
	order  string
}

// ASC means ORDER BY fields ASC
func ASC(fields ...string) OrderBy {
	return OrderBy{
		fields: fields,
		order:  "ASC",
	}
}

// DESC means ORDER BY fields DESC
func DESC(fields ...string) OrderBy {
	return OrderBy{
		fields: fields,
		order:  "DESC",
	}
}

// Selectable is a tag interface which represents SELECT XXX
type Selectable interface {
	selected()
}
