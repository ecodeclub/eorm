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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPredicate_P(t *testing.T) {
	testCases := []CommonTestCase{
		{
			name:     "default",
			builder:  New().Select(Columns("Id")).From(&TestModel{Id: 10}).Where(P("Id")),
			wantSql:  "SELECT id FROM test_model WHERE id=?;",
			wantArgs: []interface{}{int64(10)},
		},
		{
			name:     "empty",
			builder:  New().Select(Columns("Id")).From(&TestModel{Id: 10}).Where(),
			wantSql:  "SELECT id FROM test_model;",
			wantArgs: []interface{}{int64(10)},
		},
		{
			name:     "override",
			builder:  New().Select(Columns("Id")).From(&TestModel{Id: 10}).Where(P("Id").EQ(13)),
			wantSql:  "SELECT id FROM test_model WHERE `id`=?",
			wantArgs: []interface{}{13},
		},
		{
			name: "multiples",
			// 在传入多个 Predicate 的时候，我们认为它们是用 and 连接起来的
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(P("Id").LT(13), P("Id").GT(4)),
			wantSql:  "SELECT id FROM test_model WHERE `id`<? AND `id`>?",
			wantArgs: []interface{}{13, 4},
		},
		{
			name: "and",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(P("Id").LT(13).And(P("Id").GT(4))),
			wantSql:  "SELECT id FROM test_model WHERE `id`<? AND `id`>?",
			wantArgs: []interface{}{13, 4},
		},
		{
			name: "or",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(P("Id").LT(13).Or(P("Id").GT(4))),
			wantSql:  "SELECT id FROM test_model WHERE `id`<? OR `id`>?",
			wantArgs: []interface{}{13, 4},
		},
		{
			name: "and or",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(P("Id").LT(13).Or(P("Id").GT(4)).And(P("FirstName").GT("tom"))),
			wantSql:  "SELECT id FROM test_model WHERE (`id`<? OR `id`>?) AND `first_name`>?",
			wantArgs: []interface{}{13, 4, "tom"},
		},
		{
			name: "cross columns",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(P("Id").LT(13).Or(P("Age").GT(C("Id")))),
			wantSql:  "SELECT `id` FROM test_model WHERE `age`>`id`",
			wantArgs: []interface{}{13, 4, "tom"},
		},
		{
			name: "cross columns mathematical",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(P("Age").GT(C("Id").Inc(40))),
			wantSql:  "SELECT `id` FROM test_model WHERE `age`>`id`+?",
			wantArgs: []interface{}{40},
		},
		{
			name: "cross columns mathematical",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(P("Age").GT(C("Id").Times(C("Age").Inc(66)))),
			wantSql:  "SELECT `id` FROM test_model WHERE `age`>`id`*(`age`+?)",
			wantArgs: []interface{}{66},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			query, err := tc.builder.Build()
			assert.Equal(t, err, c.wantErr)
			assert.Equal(t, query.SQL, c.wantSql)
			assert.Equal(t, query.Args, c.wantArgs)
		})
	}
}

type TestModel struct {
	Id       int64
	FistName string
	Age      int8
	LastName string
}

type CommonTestCase struct {
	name     string
	builder  QueryBuilder
	wantArgs []interface{}
	wantSql  string
	wantErr  error
}
