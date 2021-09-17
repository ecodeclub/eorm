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

import (
	"fmt"
	"strings"

	"github.com/gotomicro/eql/internal"
)

// Deleter builds DELETE query
type Deleter struct {
	option  Option
	from    interface{}
	where   []Predicate
	orderby []OrderBy
	limit   int
}

// Build returns DELETE query
func (d *Deleter) Build() (*Query, error) {
	builder := strings.Builder{}
	builder.WriteString("DELETE FROM ")
	table := internal.TableName(d.from)
	builder.WriteString(" `" + table + "` ")
	if len(d.orderby) > 0 {
		builder.WriteString(" ORDER by ")
	}
	for key, v := range d.orderby {
		builder.WriteString(strings.Join(v.fields, ","))
		builder.WriteString(" " + v.order)
		if key != len(d.orderby)-1 {
			builder.WriteString(",")
		}
	}
	if d.limit != 0 {
		builder.WriteString(fmt.Sprintf(" limit %d", d.limit))
	}
	builder.WriteString(";")
	return &Query{SQL: builder.String()}, nil
}

// From accepts model definition
func (d *Deleter) From(table interface{}) *Deleter {
	d.from = table
	return d
}

// Where accepts predicates
func (d *Deleter) Where(predicates ...Predicate) *Deleter {
	return d
}

// OrderBy means "ORDER BY"
func (d *Deleter) OrderBy(orderBy ...OrderBy) *Deleter {
	d.orderby = orderBy
	return d
}

// Limit limits the number of deleted rows
func (d *Deleter) Limit(limit int) *Deleter {
	d.limit = limit
	return d
}
