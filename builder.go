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

	"github.com/gotomicro/eorm/internal/errs"
	"github.com/gotomicro/eorm/internal/model"
	"github.com/valyala/bytebufferpool"
)

// QueryBuilder is used to build a query
type QueryBuilder interface {
	Build() (*Query, error)
}

var _ Executor = &Inserter[any]{}
var _ Executor = &Updater[any]{}
var _ Executor = &Deleter[any]{}

// Executor is used to build a query
type Executor interface {
	Exec(ctx context.Context) Result
}

// Query 代表一个查询
type Query struct {
	SQL  string
	Args []any
}

// Querier 查询器，代表最基本的查询
type Querier[T any] struct {
	core
	session
	qc *QueryContext
}

// RawQuery 创建一个 Querier 实例
// 泛型参数 T 是目标类型。
// 例如，如果查询 User 的数据， 那么 T 就是 User
func RawQuery[T any](sess session, sql string, args ...any) Querier[T] {
	return Querier[T]{
		core:    sess.getCore(),
		session: sess,
		qc: &QueryContext{
			q: &Query{
				SQL:  sql,
				Args: args,
			},
			Type: RAW,
		},
	}
}

func newQuerier[T any](sess session, q *Query, meta *model.TableMeta, typ string) Querier[T] {
	return Querier[T]{
		core:    sess.getCore(),
		session: sess,
		qc: &QueryContext{
			q:    q,
			meta: meta,
			Type: typ,
		},
	}
}

// Exec 执行 SQL
func (q Querier[T]) Exec(ctx context.Context) Result {
	var handler HandleFunc = func(ctx context.Context, qc *QueryContext) *QueryResult {
		res, err := q.session.execContext(ctx, qc.q.SQL, qc.q.Args...)
		return &QueryResult{Result: res, Err: err}
	}

	ms := q.ms
	for i := len(ms) - 1; i >= 0; i-- {
		handler = ms[i](handler)
	}
	qr := handler(ctx, q.qc)
	var res sql.Result
	if qr.Result != nil {
		res = qr.Result.(sql.Result)
	}
	return Result{err: qr.Err, res: res}
}

// Get 执行查询并且返回第一行数据
// 注意在不同的数据库里面，排序可能会不同
// 在没有查找到数据的情况下，会返回 ErrNoRows
func (q Querier[T]) Get(ctx context.Context) (*T, error) {
	res := get[T](ctx, q.session, q.core, q.qc)
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Result.(*T), nil
}

type builder struct {
	core
	// 使用 bytebufferpool 以减少内存分配
	// 每次调用 Get 之后不要忘记再调用 Put
	buffer  *bytebufferpool.ByteBuffer
	meta    *model.TableMeta
	args    []interface{}
	aliases map[string]struct{}
}

func (b *builder) quote(val string) {
	_ = b.buffer.WriteByte(b.dialect.Quote)
	_, _ = b.buffer.WriteString(val)
	_ = b.buffer.WriteByte(b.dialect.Quote)
}

func (b *builder) space() {
	_ = b.buffer.WriteByte(' ')
}

func (b *builder) point() {
	_ = b.buffer.WriteByte('.')
}

func (b *builder) writeString(val string) {
	_, _ = b.buffer.WriteString(val)
}

func (b *builder) writeByte(c byte) {
	_ = b.buffer.WriteByte(c)
}

func (b *builder) end() {
	_ = b.buffer.WriteByte(';')
}

func (b *builder) comma() {
	_ = b.buffer.WriteByte(',')
}

func (b *builder) parameter(arg interface{}) {
	if b.args == nil {
		// TODO 4 may be not a good number
		b.args = make([]interface{}, 0, 4)
	}
	_ = b.buffer.WriteByte('?')
	b.args = append(b.args, arg)
}

func (b *builder) buildExpr(expr Expr) error {
	switch e := expr.(type) {
	case nil:
	case RawExpr:
		b.buildRawExpr(e)
	case Column:
		_, ok := b.aliases[e.name]
		if ok {
			b.quote(e.name)
			return nil
		}
		return b.buildColumn(e)
	case Aggregate:
		if err := b.buildHavingAggregate(e); err != nil {
			return err
		}
	case valueExpr:
		b.parameter(e.val)
	case binaryExpr:
		if err := b.buildBinaryExpr(e); err != nil {
			return err
		}
	case Predicate:
		if err := b.buildBinaryExpr(binaryExpr(e)); err != nil {
			return err
		}
	case values:
		if err := b.buildIns(e); err != nil {
			return err
		}
	case Subquery:
		return b.buildSubquery(e, false)
	case SubqueryExpr:
		b.writeString(e.pred)
		b.writeByte(' ')
		return b.buildSubquery(e.s, false)
	default:
		return errs.NewErrUnsupportedExpressionType()
	}
	return nil
}

func (b *builder) buildPredicates(predicates []Predicate) error {
	p := predicates[0]
	for i := 1; i < len(predicates); i++ {
		p = p.And(predicates[i])
	}
	return b.buildExpr(p)
}

func (b *builder) buildHavingAggregate(aggregate Aggregate) error {
	b.writeString(aggregate.fn)

	_ = b.buffer.WriteByte('(')
	if aggregate.distinct {
		b.writeString("DISTINCT ")
	}
	cMeta, ok := b.meta.FieldMap[aggregate.arg]
	if !ok {
		return errs.NewInvalidFieldError(aggregate.arg)
	}
	b.quote(cMeta.ColumnName)
	_ = b.buffer.WriteByte(')')
	return nil
}

func (b *builder) buildBinaryExpr(e binaryExpr) error {
	err := b.buildSubExpr(e.left)
	if err != nil {
		return err
	}
	b.writeString(e.op.text)
	return b.buildSubExpr(e.right)
}

func (b *builder) buildRawExpr(e RawExpr) {
	b.writeString(e.raw)
	b.args = append(b.args, e.args...)
}

func (b *builder) buildSubExpr(subExpr Expr) error {
	switch r := subExpr.(type) {
	case MathExpr:
		_ = b.buffer.WriteByte('(')
		if err := b.buildBinaryExpr(binaryExpr(r)); err != nil {
			return err
		}
		_ = b.buffer.WriteByte(')')
	case Predicate:
		_ = b.buffer.WriteByte('(')
		if err := b.buildBinaryExpr(binaryExpr(r)); err != nil {
			return err
		}
		_ = b.buffer.WriteByte(')')
	default:
		if err := b.buildExpr(r); err != nil {
			return err
		}
	}
	return nil
}

func (b *builder) buildIns(is values) error {
	_ = b.buffer.WriteByte('(')
	for idx, inVal := range is.data {
		if idx > 0 {
			_ = b.buffer.WriteByte(',')
		}

		b.args = append(b.args, inVal)
		_ = b.buffer.WriteByte('?')

	}
	_ = b.buffer.WriteByte(')')
	return nil
}

func (q Querier[T]) GetMulti(ctx context.Context) ([]*T, error) {
	res := getMulti[T](ctx, q.session, q.core, q.qc)
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Result.([]*T), nil
}

func (b *builder) buildColumn(c Column) error {
	switch table := c.table.(type) {
	case nil:
		fd, ok := b.meta.FieldMap[c.name]
		// 字段不对，或者说列不对
		if !ok {
			return errs.NewInvalidFieldError(c.name)
		}
		b.quote(fd.ColumnName)
		if c.alias != "" {
			b.aliases[c.alias] = struct{}{}
			b.writeString(" AS ")
			b.quote(c.alias)
		}
	case Table:
		m, err := b.metaRegistry.Get(table.entity)
		if err != nil {
			return err
		}
		fd, ok := m.FieldMap[c.name]
		if !ok {
			return errs.NewInvalidFieldError(c.name)
		}
		if table.alias != "" {
			b.quote(table.alias)
			b.point()
		}
		b.quote(fd.ColumnName)
		if c.alias != "" {
			b.writeString(" AS ")
			b.quote(c.alias)
		}
	default:
		return errs.NewUnsupportedTableReferenceError(table)
	}
	return nil
}

// buildSubquery 構建子查詢 SQL，
// useAlias 決定是否顯示別名，即使有別名
func (b *builder) buildSubquery(sub Subquery, useAlias bool) error {
	query, err := sub.q.Build()
	if err != nil {
		return err
	}
	b.writeByte('(')
	// 拿掉最後 ';'
	b.writeString(query.SQL[:len(query.SQL)-1])
	// 因為有 build() ，所以理應 args 也需要跟 SQL 一起處理
	if len(query.Args) > 0 {
		b.addArgs(query.Args...)
	}
	b.writeByte(')')
	if useAlias {
		b.writeString(" AS ")
		b.quote(sub.alias)
	}
	return nil
}

func (b *builder) addArgs(args ...any) {
	if b.args == nil {
		b.args = make([]any, 0, 8)
	}
	b.args = append(b.args, args...)
}
