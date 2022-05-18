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
	oerror "github.com/gotomicro/eorm/internal/errs"
	"github.com/valyala/bytebufferpool"
)

// Selector represents a select query
type Selector struct {
	builder
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

func NewSelector(db *DB) *Selector {
	return &Selector{
		builder: db.builder(),
	}
}

// Build returns Select Query
func (s *Selector) Build() (*Query, error) {
	defer bytebufferpool.Put(s.buffer)
	var err error
	s.meta, err = s.registry.Get(s.table)
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

func (s *Selector) buildOrderBy() error {
	s.writeString(" ORDER BY ")
	for i, ob := range s.orderBy {
		if i > 0 {
			s.comma()
		}
		for _, c := range ob.fields {
			cMeta, ok := s.meta.FieldMap[c]
			if !ok {
				return oerror.NewInvalidColumnError(c)
			}
			s.quote(cMeta.ColumnName)
		}
		s.space()
		s.writeString(ob.order)
	}
	return nil
}

func (s *Selector) buildGroupBy() error {
	s.writeString(" GROUP BY ")
	for i, gb := range s.groupBy {
		cMeta, ok := s.meta.FieldMap[gb]
		if !ok {
			return oerror.NewInvalidColumnError(gb)
		}
		if i > 0 {
			s.comma()
		}
		s.quote(cMeta.ColumnName)
	}
	return nil
}

func (s *Selector) buildAllColumns() {
	for i, cMeta := range s.meta.Columns {
		if i > 0 {
			s.comma()
		}
		// it should never return error, we can safely ignore it
		_ = s.buildColumn(cMeta.FieldName, "")
	}
}

// buildSelectedList users specify columns
func (s *Selector) buildSelectedList() error {
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
func (s *Selector) selectAggregate(aggregate Aggregate) error {
	s.writeString(aggregate.fn)
	s.writeByte('(')
	cMeta, ok := s.meta.FieldMap[aggregate.arg]
	s.aliases[aggregate.alias] = struct{}{}
	if !ok {
		return oerror.NewInvalidColumnError(aggregate.arg)
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

func (s *Selector) buildColumn(field, alias string) error {
	cMeta, ok := s.meta.FieldMap[field]
	if !ok {
		return oerror.NewInvalidColumnError(field)
	}
	s.quote(cMeta.ColumnName)
	if alias != "" {
		s.aliases[alias] = struct{}{}
		s.writeString(" AS ")
		s.quote(alias)
	}
	return nil
}

// Columns specifies the table which must be pointer of structure
func (s *Selector) Select(columns ...Selectable) *Selector {
	s.columns = columns
	return s
}

// From specifies the table which must be pointer of structure
func (s *Selector) From(table interface{}) *Selector {
	s.table = table
	return s
}

// Where accepts predicates
func (s *Selector) Where(predicates ...Predicate) *Selector {
	s.where = predicates
	return s
}

// Distinct indicates using keyword DISTINCT
func (s *Selector) Distinct() *Selector {
	s.distinct = true
	return s
}

// Having accepts predicates
func (s *Selector) Having(predicates ...Predicate) *Selector {
	s.having = predicates
	return s
}

// GroupBy means "GROUP BY"
func (s *Selector) GroupBy(columns ...string) *Selector {
	s.groupBy = columns
	return s
}

// OrderBy means "ORDER BY"
func (s *Selector) OrderBy(orderBys ...OrderBy) *Selector {
	s.orderBy = orderBys
	return s
}

// Limit limits the size of result set
func (s *Selector) Limit(limit int) *Selector {
	s.limit = limit
	return s
}

// Offset was used by "LIMIT"
func (s *Selector) Offset(offset int) *Selector {
	s.offset = offset
	return s
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
