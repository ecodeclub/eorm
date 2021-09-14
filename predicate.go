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

type op struct {

}

// Predicate will be used in Where Or Having
type Predicate binaryExpr

// P creates a Predicate
// left could be string or Expr
func P(left interface{}) Predicate {
	panic("implement me")
}

// Not indicates "NOT"
func Not(p Predicate) Predicate {
	panic("implement me")
}

// And indicates "AND"
func (p Predicate) And(pred Predicate) Predicate {
	panic("implement me")
}

// Or indicates "OR"
func (p Predicate) Or(pred Predicate) Predicate {
	panic("implement me")
}

// EQ =
func (p Predicate) EQ(val interface{}) Predicate {
	panic("implement")
}

// LT <
func (p Predicate) LT(val interface{}) Predicate {
	panic("implement me")
}

// GT >
func (p Predicate) GT(val interface{}) Predicate {
	panic("implement me")
}