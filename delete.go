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

// Deleter builds DELETE query
type Deleter struct {
	builder
	table interface{}
	where []Predicate
}

// Build returns DELETE query
func (d *Deleter) Build() (*Query, error) {
	_, err := d.buffer.WriteString("DELETE FROM ")
	if err != nil {
		return nil, err
	}
	d.meta, err = d.registry.Get(d.table)
	if err != nil {
		return nil, err
	}

	d.quote(d.meta.TableName)
	if len(d.where) > 0 {
		_, err = d.buffer.WriteString(" WHERE ")
		if err != nil {
			return nil, err
		}
		err = d.buildPredicates(d.where)
		if err != nil {
			return nil, err
		}
	}
	d.end()
	return &Query{SQL: d.buffer.String(), Args: d.args}, nil
}

// From accepts model definition
func (d *Deleter) From(table interface{}) *Deleter {
	d.table = table
	return d
}

// Where accepts predicates
func (d *Deleter) Where(predicates ...Predicate) *Deleter {
	d.where = predicates
	return d
}
