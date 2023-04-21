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

// type op Operator.Op
var (
	opLT      = operator.OpLT
	opLTEQ    = operator.OpLTEQ
	opGT      = operator.OpGT
	opGTEQ    = operator.OpGTEQ
	opEQ      = operator.OpEQ
	opNEQ     = operator.OpNEQ
	opAdd     = operator.OpAdd
	opMulti   = operator.OpMulti
	opAnd     = operator.OpAnd
	opOr      = operator.OpOr
	opNot     = operator.OpNot
	opIn      = operator.OpIn
	opNotIN   = operator.OpNotIN
	opFalse   = operator.OpFalse
	opLike    = operator.OpLike
	opNotLike = operator.OpNotLike
	opExist   = operator.OpExist
)

// Predicate will be used in Where Or Having
type Predicate binaryExpr

var emptyPredicate = Predicate{}

func (Predicate) expr() (string, error) {
	return "", nil
}

// Exist indicates "Exist"
func Exist(sub Subquery) Predicate {
	return Predicate{
		op:    opExist,
		right: sub,
	}
}

// Not indicates "NOT"
func Not(p Predicate) Predicate {
	return Predicate{
		left:  Raw(""),
		op:    opNot,
		right: p,
	}
}

// And indicates "AND"
func (p Predicate) And(pred Predicate) Predicate {
	return Predicate{
		left:  p,
		op:    opAnd,
		right: pred,
	}
}

// Or indicates "OR"
func (p Predicate) Or(pred Predicate) Predicate {
	return Predicate{
		left:  p,
		op:    opOr,
		right: pred,
	}
}
