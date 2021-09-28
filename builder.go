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

import "strings"

// QueryBuilder is used to build a query
type QueryBuilder interface {
	Build() (*Query, error)
}

// Query represents a query
type Query struct {
	SQL string
	Args []interface{}
}

type builder struct {
	registry MetaRegistry
	dialect Dialect
	// TODO using buffer cache
	buffer *strings.Builder
	meta *TableMeta
	args []interface{}
}

func (b builder) quote(val string) {
	b.buffer.WriteByte(b.dialect.quote)
	b.buffer.WriteString(val)
	b.buffer.WriteByte(b.dialect.quote)
}

func (b builder) space() {
	b.buffer.WriteByte(' ')
}

func (b builder) end() {
	b.buffer.WriteByte(';')
}

func (b builder) comma() {
	b.buffer.WriteByte(',')
}

func (b *builder) parameter(arg interface{}) {
	if b.args == nil {
		// TODO 4 may be not a good number
		b.args = make([]interface{}, 0, 4)
	}
	b.buffer.WriteByte('?')
	b.args = append(b.args, arg)
}