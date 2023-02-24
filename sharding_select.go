// Copyright 2021 ecodehub
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
	"fmt"
	"sync"

	"github.com/ecodehub/eorm/internal/errs"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/sync/errgroup"
)

type ShardingSelector[T any] struct {
	selectorBuilder
	table *T
	db    *ShardingDB
	lock  sync.Mutex
}

func NewShardingSelector[T any](db *ShardingDB) *ShardingSelector[T] {
	return &ShardingSelector[T]{
		selectorBuilder: selectorBuilder{
			builder: builder{
				core:   db.getCore(),
				buffer: bytebufferpool.Get(),
			},
		},
		db: db,
	}
}

func (s *ShardingSelector[T]) Build() ([]*ShardingQuery, error) {
	var err error
	if s.meta == nil {
		s.meta, err = s.metaRegistry.Get(new(T))
		if err != nil {
			return nil, err
		}
	}
	if s.meta.ShardingKey == "" {
		return nil, errs.ErrMissingShardingKey
	}
	dsts, err := s.findDsts()
	if err != nil {
		return nil, err
	}
	res := make([]*ShardingQuery, 0, len(dsts))
	for _, dst := range dsts {
		query, err := s.buildQuery(dst.DB, dst.Table)
		if err != nil {
			return nil, err
		}
		res = append(res, query)
		s.args = make([]any, 0, 8)
	}
	return res, nil
}

func (s *ShardingSelector[T]) buildQuery(db, tbl string) (*ShardingQuery, error) {
	defer bytebufferpool.Put(s.buffer)
	var err error
	s.writeString("SELECT ")
	if len(s.columns) == 0 {
		if err = s.buildAllColumns(); err != nil {
			return nil, err
		}
	} else {
		err = s.buildSelectedList()
		if err != nil {
			return nil, err
		}
	}
	s.writeString(" FROM ")
	s.quote(db)
	s.writeByte('.')
	s.quote(tbl)

	if len(s.where) > 0 {
		s.writeString(" WHERE ")
		p := s.where[0]
		for i := 1; i < len(s.where); i++ {
			p = p.And(s.where[i])
		}
		if err = s.buildExpr(p); err != nil {
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
		p := s.having[0]
		for i := 1; i < len(s.having); i++ {
			p = p.And(s.having[i])
		}
		if err = s.buildExpr(p); err != nil {
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

	return &ShardingQuery{SQL: s.buffer.String(), Args: s.args, DB: db}, nil
}

func (s *ShardingSelector[T]) findDsts() ([]Dst, error) {
	//  通过遍历 pre 查找目标 shardingkey
	if len(s.where) > 0 {
		pre := s.where[0]
		for i := 1; i < len(s.where)-1; i++ {
			pre = pre.And(s.where[i])
		}
		return s.findDstByPredicate(pre)
	}
	return nil, nil
}

func (s *ShardingSelector[T]) findDstByPredicate(pre Predicate) ([]Dst, error) {
	var res []Dst
	switch pre.op {
	case opAnd:
		left, err := s.findDstByPredicate(pre.left.(Predicate))
		if err != nil {
			return nil, err
		}
		if len(left) == 0 {
			return s.findDstByPredicate(pre.right.(Predicate))
		}
		right, err := s.findDstByPredicate(pre.right.(Predicate))
		if err != nil {
			return nil, err
		}
		if len(right) == 0 {
			return left, nil
		}
		return s.mergeAnd(left, right), nil
	case opOr:
		left, err := s.findDstByPredicate(pre.left.(Predicate))
		if err != nil {
			return nil, err
		}
		if len(left) == 0 {
			return s.db.broadcast(), nil
		}
		right, err := s.findDstByPredicate(pre.right.(Predicate))
		if err != nil {
			return nil, err
		}
		if len(right) == 0 {
			return s.db.broadcast(), nil
		}
		return s.mergeOR(left, right), nil
	case opEQ:
		col, isCol := pre.left.(Column)
		right, isVals := pre.right.(valueExpr)
		if !isCol || !isVals {
			return nil, errs.ErrUnsupportedTooComplexQuery
		}
		if col.name == s.meta.ShardingKey {
			shardingDB, err := s.meta.DBShardingFunc(right.val)
			if err != nil {
				return nil, errs.ErrExcShardingAlgorithm
			}
			shardingTbl, err := s.meta.TableShardingFunc(right.val)
			if err != nil {
				return nil, errs.ErrExcShardingAlgorithm
			}
			_, existDB := s.db.DBs[shardingDB]
			if !existDB {
				return nil, errs.ErrNotFoundTargetDB
			}
			_, existTbl := s.db.Tables[shardingTbl]
			if !existTbl {
				return nil, errs.ErrNotFoundTargetTable
			}
			dst := Dst{DB: shardingDB, Table: shardingTbl}
			res = append(res, dst)
		}
	default:
		return nil, errs.NewUnsupportedOperatorError(pre.op.text)
	}
	return res, nil
}

func (*ShardingSelector[T]) mergeAnd(left, right []Dst) []Dst {
	res := make([]Dst, 0, len(left)+len(right))
	for _, r := range right {
		exist := false
		for _, l := range left {
			if r.DB == l.DB && r.Table == l.Table {
				exist = true
			}
		}
		if exist {
			res = append(res, r)
		}
	}
	return res
}

func (*ShardingSelector[T]) mergeOR(left, right []Dst) []Dst {
	res := make([]Dst, 0, len(left)+len(right))
	m := make(map[string]bool, 8)
	for _, r := range right {
		for _, l := range left {
			if r.DB != l.DB || r.Table != l.Table {
				tbl := fmt.Sprintf("%s_%s", l.DB, l.Table)
				if _, ok := m[tbl]; ok {
					continue
				}
				res = append(res, l)
				m[tbl] = true
			}
		}
		res = append(res, r)
	}
	return res
}

func (s *ShardingSelector[T]) buildAllColumns() error {
	for i, cMeta := range s.meta.Columns {
		_ = s.buildColumns(i, cMeta.FieldName)
	}
	return nil
}

func (s *ShardingSelector[T]) buildSelectedList() error {
	for i, selectable := range s.columns {
		if i > 0 {
			s.comma()
		}
		switch expr := selectable.(type) {
		case Column:
			err := s.builder.buildColumn(expr)
			if err != nil {
				return errs.NewInvalidFieldError(expr.name)
			}
		case columns:
			for j, c := range expr.cs {
				err := s.buildColumns(j, c)
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
func (s *ShardingSelector[T]) selectAggregate(aggregate Aggregate) error {
	s.writeString(aggregate.fn)

	s.writeByte('(')
	if aggregate.distinct {
		s.writeString("DISTINCT ")
	}
	cMeta, ok := s.meta.FieldMap[aggregate.arg]
	if !ok {
		return errs.NewInvalidFieldError(aggregate.arg)
	}
	if aggregate.table != nil {
		if alias := aggregate.table.getAlias(); alias != "" {
			s.quote(alias)
			s.point()
		}
	}
	s.quote(cMeta.ColumnName)
	s.writeByte(')')
	if aggregate.alias != "" {
		s.writeString(" AS ")
		s.quote(aggregate.alias)
	}
	return nil
}

func (s *ShardingSelector[T]) buildColumns(index int, name string) error {
	if index > 0 {
		s.comma()
	}
	cMeta, ok := s.meta.FieldMap[name]
	if !ok {
		return errs.NewInvalidFieldError(name)
	}
	s.quote(cMeta.ColumnName)
	return nil
}

func (s *ShardingSelector[T]) buildExpr(expr Expr) error {
	switch exp := expr.(type) {
	case nil:
	case Column:
		exp.alias = ""
		_ = s.buildColumn(exp)
	case valueExpr:
		s.parameter(exp.val)
	case RawExpr:
		s.buildRawExpr(exp)
	case Predicate:
		if err := s.buildBinaryExpr(binaryExpr(exp)); err != nil {
			return err
		}
	default:
		return errs.NewErrUnsupportedExpressionType()
	}
	return nil
}

func (s *ShardingSelector[T]) buildOrderBy() error {
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

func (s *ShardingSelector[T]) buildGroupBy() error {
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

func (s *ShardingSelector[T]) Get(ctx context.Context) (*T, error) {
	qs, err := s.Limit(1).Build()
	if err != nil {
		return nil, err
	}
	if len(qs) == 0 {
		return nil, errs.ErrNotGenShardingQuery
	}
	// TODO 要确保前面的改写 SQL 只能生成一个 SQL
	if len(qs) > 1 {
		return nil, errs.ErrOnlyResultOneQuery
	}
	query := qs[0]
	// TODO 利用 ctx 传递 DB name
	ctx = CtxWithDBName(ctx, query.DB)
	row, err := s.db.queryContext(ctx, query.SQL, query.Args...)
	if err != nil {
		return nil, err
	}
	if !row.Next() {
		return nil, ErrNoRows
	}
	tp := new(T)
	val := s.valCreator.NewBasicTypeValue(tp, s.meta)
	if err = val.SetColumns(row); err != nil {
		return nil, err
	}
	return tp, nil
}

func (s *ShardingSelector[T]) GetMulti(ctx context.Context) ([]*T, error) {
	qs, err := s.Build()
	if err != nil {
		return nil, err
	}
	var rowsSlice []*sql.Rows
	var eg errgroup.Group
	for _, query := range qs {
		q := query
		eg.Go(func() error {
			s.lock.Lock()
			defer s.lock.Unlock()
			// TODO 利用 ctx 传递 DB name
			lctx := CtxWithDBName(ctx, q.DB)
			rows, err := s.db.queryContext(lctx, q.SQL, q.Args...)
			if err == nil {
				rowsSlice = append(rowsSlice, rows)
			}
			return err
		})
	}
	err = eg.Wait()
	if err != nil {
		return nil, err
	}
	var res []*T
	for _, rows := range rowsSlice {
		for rows.Next() {
			tp := new(T)
			val := s.valCreator.NewBasicTypeValue(tp, s.meta)
			if err = val.SetColumns(rows); err != nil {
				return nil, err
			}
			res = append(res, tp)
		}
	}
	return res, nil
}

// Select 指定查询的列。
// 列可以是物理列，也可以是聚合函数，或者 RawExpr
func (s *ShardingSelector[T]) Select(columns ...Selectable) *ShardingSelector[T] {
	s.columns = columns
	return s
}

// From specifies the table which must be pointer of structure
func (s *ShardingSelector[T]) From(tbl *T) *ShardingSelector[T] {
	s.table = tbl
	return s
}

// Where accepts predicates
func (s *ShardingSelector[T]) Where(predicates ...Predicate) *ShardingSelector[T] {
	s.where = predicates
	return s
}

// Having accepts predicates
func (s *ShardingSelector[T]) Having(predicates ...Predicate) *ShardingSelector[T] {
	s.having = predicates
	return s
}

// GroupBy means "GROUP BY"
func (s *ShardingSelector[T]) GroupBy(columns ...string) *ShardingSelector[T] {
	s.groupBy = columns
	return s
}

// OrderBy means "ORDER BY"
func (s *ShardingSelector[T]) OrderBy(orderBys ...OrderBy) *ShardingSelector[T] {
	s.orderBy = orderBys
	return s
}

// Limit limits the size of result set
func (s *ShardingSelector[T]) Limit(limit int) *ShardingSelector[T] {
	s.limit = limit
	return s
}

// Offset was used by "LIMIT"
func (s *ShardingSelector[T]) Offset(offset int) *ShardingSelector[T] {
	s.offset = offset
	return s
}
