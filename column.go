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

// Column represents column
// it could have alias
// in general, we use it in two ways
// 1. specify the column in query
// 2. it's the start point of building complex expression
type Column struct {
	table TableReference
	name  string
	alias string
}

// C specify column
func C(c string) Column {
	return Column{
		name: c,
	}
}

// EQ =
func (c Column) EQ(val interface{}) Predicate {
	return Predicate{
		left:  c,
		op:    opEQ,
		right: valueOf(val),
	}
}

// NEQ !=
func (c Column) NEQ(val interface{}) Predicate {
	return Predicate{
		left:  c,
		op:    opNEQ,
		right: valueOf(val),
	}
}

// LT <
func (c Column) LT(val interface{}) Predicate {
	return Predicate{
		left:  c,
		op:    opLT,
		right: valueOf(val),
	}
}

// LTEQ <=
func (c Column) LTEQ(val interface{}) Predicate {
	return Predicate{
		left:  c,
		op:    opLTEQ,
		right: valueOf(val),
	}
}

// GT >
func (c Column) GT(val interface{}) Predicate {
	return Predicate{
		left:  c,
		op:    opGT,
		right: valueOf(val),
	}
}

// GTEQ >=
func (c Column) GTEQ(val interface{}) Predicate {
	return Predicate{
		left:  c,
		op:    opGTEQ,
		right: valueOf(val),
	}
}

// As means alias
func (c Column) As(alias string) Selectable {
	return Column{
		table: c.table,
		name:  c.name,
		alias: alias,
	}
}

// Like -> LIKE %XXX 、_x_ 、xx[xx-xx] 、xx[^xx-xx]
func (c Column) Like(val interface{}) Predicate {
	return Predicate{
		left:  c,
		op:    opLike,
		right: valueOf(val),
	}
}

// NotLike -> NOT LIKE %XXX 、_x_ 、xx[xx-xx] 、xx[^xx-xx]
func (c Column) NotLike(val interface{}) Predicate {
	return Predicate{
		left:  c,
		op:    opNotLike,
		right: valueOf(val),
	}
}

// Add generate an additive expression
func (c Column) Add(val interface{}) MathExpr {
	return MathExpr{
		left:  c,
		op:    opAdd,
		right: valueOf(val),
	}
}

// Multi generate a multiplication expression
func (c Column) Multi(val interface{}) MathExpr {
	return MathExpr{
		left:  c,
		op:    opMulti,
		right: valueOf(val),
	}
}

func (Column) assign() {
	panic("implement me")
}

func (Column) expr() (string, error) {
	panic("implement me")
}

func (Column) selected() {
	panic("implement me")
}

type columns struct {
	cs []string
}

func (columns) selected() {
	panic("implement me")
}

func (columns) assign() {
	panic("implement me")
}

// Columns specify columns
func Columns(cs ...string) columns {
	return columns{
		cs: cs,
	}
}

// In 方法没有元素传入，会被认为是false，被解释成where false这种形式
// 支持一個 Subquery 子查詢
func (c Column) In(data ...any) Predicate {
	if len(data) == 0 {
		return Predicate{
			op: opFalse,
		}
	}

	switch data[0].(type) {
	case Subquery:
		return Predicate{
			left:  c,
			op:    opIn,
			right: data[0].(Subquery),
		}
	default:
		return Predicate{
			left: c,
			op:   opIn,
			right: values{
				data: data,
			},
		}
	}
}

// NotIn 方法没有元素传入，会被认为是false，被解释成where false这种形式
func (c Column) NotIn(data ...any) Predicate {
	if len(data) == 0 {
		return Predicate{
			op: opFalse,
		}
	}
	return Predicate{
		left: c,
		op:   opNotIN,
		right: values{
			data: data,
		},
	}
}

type values struct {
	data []any
}

func (values) expr() (string, error) {
	panic("implement me")
}
