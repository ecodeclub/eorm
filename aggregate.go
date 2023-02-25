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

// Aggregate represents aggregate expression, including AVG, MAX, MIN...
type Aggregate struct {
	table    TableReference
	fn       string
	arg      string
	alias    string
	distinct bool
}

// As 指定别名。一般情况下，这个别名应该等同于列名，我们会将这个列名映射过去对应的字段名。
// 例如说 alias= avg_age，默认情况下，我们会找 AvgAge 这个字段来接收值。
func (a Aggregate) As(alias string) Selectable {
	return Aggregate{
		fn:    a.fn,
		arg:   a.arg,
		alias: alias,
		table: a.table,
	}
}

// Avg represents AVG
func Avg(c string) Aggregate {
	return Aggregate{
		fn:  "AVG",
		arg: c,
	}
}

// Max represents MAX
func Max(c string) Aggregate {
	return Aggregate{
		fn:  "MAX",
		arg: c,
	}
}

// Min represents MIN
func Min(c string) Aggregate {
	return Aggregate{
		fn:  "MIN",
		arg: c,
	}
}

// Count represents COUNT
func Count(c string) Aggregate {
	return Aggregate{
		fn:  "COUNT",
		arg: c,
	}
}

// Sum represents SUM
func Sum(c string) Aggregate {
	return Aggregate{
		fn:  "SUM",
		arg: c,
	}
}

// CountDistinct represents COUNT(DISTINCT XXX)
func CountDistinct(col string) Aggregate {
	a := Count(col)
	a.distinct = true
	return a
}

// AvgDistinct represents AVG(DISTINCT XXX)
func AvgDistinct(col string) Aggregate {
	a := Avg(col)
	a.distinct = true
	return a
}

// SumDistinct represents SUM(DISTINCT XXX)
func SumDistinct(col string) Aggregate {
	a := Sum(col)
	a.distinct = true
	return a
}

func (Aggregate) selected() {}

func (a Aggregate) EQ(val interface{}) Predicate {
	return Predicate{
		left:  a,
		op:    opEQ,
		right: valueOf(val),
	}
}
func (a Aggregate) NEQ(val interface{}) Predicate {
	return Predicate{
		left:  a,
		op:    opNEQ,
		right: valueOf(val),
	}
}

// LT <
func (a Aggregate) LT(val interface{}) Predicate {
	return Predicate{
		left:  a,
		op:    opLT,
		right: valueOf(val),
	}
}

// LTEQ <=
func (a Aggregate) LTEQ(val interface{}) Predicate {
	return Predicate{
		left:  a,
		op:    opLTEQ,
		right: valueOf(val),
	}
}

// GT >
func (a Aggregate) GT(val interface{}) Predicate {
	return Predicate{
		left:  a,
		op:    opGT,
		right: valueOf(val),
	}
}

// GTEQ >=
func (a Aggregate) GTEQ(val interface{}) Predicate {
	return Predicate{
		left:  a,
		op:    opGTEQ,
		right: valueOf(val),
	}
}

func (Aggregate) expr() (string, error) {
	return "", nil
}
