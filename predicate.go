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

type op struct {
	symbol string
	text   string
}

var (
	opLT   = op{symbol: "<", text: "<"}
	opLTEQ = op{symbol: "<=", text: "<="}
	opGT   = op{symbol: ">", text: ">"}
	opGTEQ = op{symbol: ">=", text: ">="}
	opEQ   = op{symbol: "=", text: "="}
	opNEQ  = op{symbol: "!=", text: "!="}
	opAdd  = op{symbol: "+", text: "+"}
	// opIn   = op{symbol: "IN", text: " IN "}
	// opMinus = op{symbol:"-", text: "-"}
	opMulti = op{symbol: "*", text: "*"}
	// opDiv = op{symbol:"/", text: "/"}
	opAnd     = op{symbol: "AND", text: " AND "}
	opOr      = op{symbol: "OR", text: " OR "}
	opNot     = op{symbol: "NOT", text: "NOT "}
	opIn      = op{symbol: "IN", text: " IN "}
	opNotIN   = op{symbol: "NOT IN", text: " NOT IN "}
	opFalse   = op{symbol: "FALSE", text: "FALSE"}
	opLike    = op{symbol: "LIKE", text: " LIKE "}
	opNotLike = op{symbol: "NOT LIKE", text: " NOT LIKE "}
	opExist   = op{symbol: "EXIST", text: "EXIST "}
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
