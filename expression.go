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

import operator "github.com/ecodeclub/eorm/internal/operator"

// Expr is the top interface. It represents everything.
type Expr interface {
	expr() (string, error)
}

// RawExpr uses string alias Expr
type RawExpr struct {
	raw  string
	args []interface{}
}

// Raw just take expr alias Expr
func Raw(expr string, args ...interface{}) RawExpr {
	return RawExpr{
		raw:  expr,
		args: args,
	}
}

func (r RawExpr) expr() (string, error) {
	return r.raw, nil
}

// AsPredicate 将会返回一个 Predicate，RawExpr 将会作为这个 Predicate 的左边部分
// eorm 将不会校验任何从 RawExpr 生成的 Predicate
func (r RawExpr) AsPredicate() Predicate {
	return Predicate{
		left: r,
	}
}

func (RawExpr) selected() {}

type binaryExpr struct {
	left  Expr
	op    operator.Op
	right Expr
}

func (binaryExpr) expr() (string, error) {
	return "", nil
}

type MathExpr binaryExpr

func (m MathExpr) Add(val interface{}) Expr {
	return MathExpr{
		left:  m,
		op:    opAdd,
		right: valueOf(val),
	}
}

func (m MathExpr) Multi(val interface{}) MathExpr {
	return MathExpr{
		left:  m,
		op:    opMulti,
		right: valueOf(val),
	}
}

func (MathExpr) expr() (string, error) {
	return "", nil
}

func valueOf(val interface{}) Expr {
	switch v := val.(type) {
	case Expr:
		return v
	default:
		return valueExpr{val: val}
	}
}

type SubqueryExpr struct {
	s Subquery
	// 謂詞： ALL、ANY、SOME
	pred string
}

func (SubqueryExpr) expr() (string, error) {
	panic("implement me")
}

func Any(sub Subquery) SubqueryExpr {
	return SubqueryExpr{
		s:    sub,
		pred: "ANY",
	}
}

func All(sub Subquery) SubqueryExpr {
	return SubqueryExpr{
		s:    sub,
		pred: "ALL",
	}
}

func Some(sub Subquery) SubqueryExpr {
	return SubqueryExpr{
		s:    sub,
		pred: "SOME",
	}
}
