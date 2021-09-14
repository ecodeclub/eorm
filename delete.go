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

// Deleter builds DELETE query
type Deleter struct {

}

// Build returns DELETE query
func (d *Deleter) Build() (*Query, error) {
	panic("implement me")
}

// From accepts model definition
func (d *Deleter) From(table interface{}) *Deleter {
	panic("implement me")
}

// Where accepts predicates
func (d *Deleter) Where(predicates...Predicate) *Deleter {
	panic("implement me")
}

// OrderBy means "ORDER BY"
func (d *Deleter) OrderBy(orderBy... OrderBy) *Deleter {
	panic("implement me")
}

// Limit limits the number of deleted rows
func (d *Deleter) Limit(limit int) *Deleter {
	panic("implement me")
}