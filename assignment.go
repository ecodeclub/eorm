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

// Assignable represents that something could be used alias "assignment" statement
type Assignable interface {
	assign()
}

// Assignment represents assignment statement
type Assignment binaryExpr

func Assign(column string, value interface{}) Assignment {
	var expr Expr
	switch v := value.(type) {
	case Expr:
		expr = v
	default:
		expr = valueExpr{val: v}
	}
	return Assignment{left: C(column), op: opEQ, right: expr}
}

func (Assignment) assign() {
	panic("implement me")
}

type valueExpr struct {
	val interface{}
}

func (valueExpr) expr() (string, error) {
	return "", nil
}
