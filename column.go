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

// Column represents column
// it could have alias
// in general, we use it in two ways
// 1. specify the column in query
// 2. it's the start point of building complex expression
type Column struct {
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
		name:  c.name,
		alias: alias,
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

func (c columns) selected() {
	panic("implement me")
}

func (c columns) assign() {
	panic("implement me")
}

// Columns specify columns
func Columns(cs ...string) columns {
	return columns{
		cs: cs,
	}
}

func (c Column) In(ins ...any) Predicate {
	return Predicate{
		left: c,
		op:   opIn,
		right: Ins{
			ins: ins,
		},
	}
}
func (c Column) NotIn(ins ...any) Predicate {
	return Predicate{
		left: c,
		op:   opNotIN,
		right: Ins{
			ins: ins,
		},
	}
}

type Ins struct {
	ins []any
}

func (i Ins) expr() (string, error) {
	panic("implement me")
}
