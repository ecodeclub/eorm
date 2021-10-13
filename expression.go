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

package eql

// Expr is the top interface. It represents everything.
type Expr interface {
	expr() (string, error)
}

// RawExpr uses string as Expr
type RawExpr string

// Raw just take expr as Expr
func Raw(expr string) RawExpr {
	return RawExpr(expr)
}


func (r RawExpr) expr() (string, error) {
	return string(r), nil
}

func (RawExpr) selected() {}

type binaryExpr struct {
	left Expr
	op op
	right Expr
}

func (binaryExpr) expr() (string, error) {
	return "", nil
}

type MathExpr binaryExpr

func (m MathExpr) Add(val interface{}) Expr {
	return MathExpr{
		left: m,
		op: opAdd,
		right: valueOf(val),
	}
}

func (m MathExpr) Multi(val interface{}) MathExpr {
	return MathExpr{
		left: m,
		op: opMulti,
		right: valueOf(val),
	}
}

func (MathExpr) expr() (string, error) {
	return "", nil
}