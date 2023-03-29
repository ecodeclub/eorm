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

package Operator

type Op struct {
	Symbol string
	Text   string
}

var (
	OpLT   = Op{Symbol: "<", Text: "<"}
	OpLTEQ = Op{Symbol: "<=", Text: "<="}
	OpGT   = Op{Symbol: ">", Text: ">"}
	OpGTEQ = Op{Symbol: ">=", Text: ">="}
	OpEQ   = Op{Symbol: "=", Text: "="}
	OpNEQ  = Op{Symbol: "!=", Text: "!="}
	OpAdd  = Op{Symbol: "+", Text: "+"}
	// OpIn   = Op{Symbol: "IN", Text: " IN "}
	// OpMinus = Op{Symbol:"-", Text: "-"}
	OpMulti = Op{Symbol: "*", Text: "*"}
	// OpDiv = Op{Symbol:"/", Text: "/"}
	OpAnd     = Op{Symbol: "AND", Text: " AND "}
	OpOr      = Op{Symbol: "OR", Text: " OR "}
	OpNot     = Op{Symbol: "NOT", Text: "NOT "}
	OpIn      = Op{Symbol: "IN", Text: " IN "}
	OpNotIN   = Op{Symbol: "NOT IN", Text: " NOT IN "}
	OpFalse   = Op{Symbol: "FALSE", Text: "FALSE"}
	OpLike    = Op{Symbol: "LIKE", Text: " LIKE "}
	OpNotLike = Op{Symbol: "NOT LIKE", Text: " NOT LIKE "}
	OpExist   = Op{Symbol: "EXIST", Text: "EXIST "}
)
