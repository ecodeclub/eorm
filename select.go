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

// Selector represents a select query
type Selector struct {

}

// Build returns Select Query
func (s *Selector) Build() (*Query, error) {
	panic("implement me")
}

// From specifies the table
func (*Selector) From(table interface{}) *Selector {
	panic("implement me")
}

// Where accepts predicates
func (*Selector) Where(predicates...Predicate) *Selector {
	panic("implement me")
}

// Distinct indicates using keyword DISTINCT
func (*Selector) Distinct() *Selector {
	panic("implement me")
}

// Having accepts predicates
func (*Selector) Having(predicates...Predicate) *Selector {
	panic("implement me")
}

// GroupBy means "GROUP BY"
func (*Selector) GroupBy(columns...string) *Selector {
	panic("implement me")
}

// OrderBy means "ORDER BY"
func (*Selector) OrderBy(orderBys...OrderBy) *Selector {
	panic("implement")
}

// Limit limits the size of result set
func (*Selector) Limit(limit int) *Selector {
	panic("implement me")
}

// Offset was used by "LIMIT"
func (*Selector) Offset(offset int) *Selector {
	panic("implement me")
}

// OrderBy specify fields and ASC
type OrderBy struct {
	fields []string
	asc bool
}

// ASC means ORDER BY fields ASC
func ASC(fields...string) OrderBy {
	panic("implement me")
}

// DESC means ORDER BY fields DESC
func DESC(fields...string) OrderBy {
	panic("implement me")
}

// Selectable is a tag interface which represents SELECT XXX
type Selectable interface {
	selected()
}