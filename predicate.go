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

import Operator "github.com/ecodeclub/eorm/internal/operator"

//type op struct {
//	Symbol string
//	Text   string
//}

type op Operator.Op

var (
	opLT   = op{Symbol: "<", Text: "<"}
	opLTEQ = op{Symbol: "<=", Text: "<="}
	opGT   = op{Symbol: ">", Text: ">"}
	opGTEQ = op{Symbol: ">=", Text: ">="}
	opEQ   = op{Symbol: "=", Text: "="}
	opNEQ  = op{Symbol: "!=", Text: "!="}
	opAdd  = op{Symbol: "+", Text: "+"}
	// opIn   = op{Symbol: "IN", Text: " IN "}
	// opMinus = op{Symbol:"-", Text: "-"}
	opMulti = op{Symbol: "*", Text: "*"}
	// opDiv = op{Symbol:"/", Text: "/"}
	opAnd     = op{Symbol: "AND", Text: " AND "}
	opOr      = op{Symbol: "OR", Text: " OR "}
	opNot     = op{Symbol: "NOT", Text: "NOT "}
	opIn      = op{Symbol: "IN", Text: " IN "}
	opNotIN   = op{Symbol: "NOT IN", Text: " NOT IN "}
	opFalse   = op{Symbol: "FALSE", Text: "FALSE"}
	opLike    = op{Symbol: "LIKE", Text: " LIKE "}
	opNotLike = op{Symbol: "NOT LIKE", Text: " NOT LIKE "}
	opExist   = op{Symbol: "EXIST", Text: "EXIST "}
)

// Predicate will be used in Where Or Having
type Predicate binaryExpr

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
