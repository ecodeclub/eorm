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

// Aggregate represents aggregate expression, including AVG, MAX, MIN...
type Aggregate struct {
	fn    string
	arg   string
	alias string
}

// As specifies the alias
func (a Aggregate) As(alias string) Selectable {
	return Aggregate{
		fn:    a.fn,
		arg:   a.arg,
		alias: alias,
	}
}

// Avg represents AVG
func Avg(c string) Aggregate {
	return Aggregate{
		fn:  "AVG",
		arg: c,
	}
}

func Max(c string) Aggregate {
	return Aggregate{
		fn:  "MAX",
		arg: c,
	}
}

func Min(c string) Aggregate {
	return Aggregate{
		fn:  "MIN",
		arg: c,
	}
}

func Count(c string) Aggregate {
	return Aggregate{
		fn:  "COUNT",
		arg: c,
	}
}

func Sum(c string) Aggregate {
	return Aggregate{
		fn:  "SUM",
		arg: c,
	}
}

func (a Aggregate) selected() {}

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
