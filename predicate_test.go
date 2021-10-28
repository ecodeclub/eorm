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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPredicate_C(t *testing.T) {
	testCases := []CommonTestCase {
		{
			name: "empty",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).Where(),
			wantSql: "SELECT `id` FROM `test_model`;",
		},
		{
			name: "multiples",
			// 在传入多个 Predicate 的时候，我们认为它们是用 and 连接起来的
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(C("Id").LT(13), C("Id").GT(4)),
			wantSql: "SELECT `id` FROM `test_model` WHERE (`id`<?) AND (`id`>?);",
			wantArgs: []interface{}{13, 4},
		},
		{
			name: "and",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(C("Id").LT(13).And(C("Id").GT(4))),
			wantSql: "SELECT `id` FROM `test_model` WHERE (`id`<?) AND (`id`>?);",
			wantArgs: []interface{}{13, 4},
		},
		{
			name: "or",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(C("Id").LT(13).Or(C("Id").GT(4))),
			wantSql: "SELECT `id` FROM `test_model` WHERE (`id`<?) OR (`id`>?);",
			wantArgs: []interface{}{13, 4},
		},
		{
			name: "mot",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(Not(C("Id").LT(13).Or(C("Id").GT(4)))),
			wantSql: "SELECT `id` FROM `test_model` WHERE NOT ((`id`<?) OR (`id`>?));",
			wantArgs: []interface{}{13, 4},
		},
		{
			name: "and or",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(C("Id").LT(13).Or(C("Id").GT(4)).And(C("FirstName").GT("tom"))),
			wantSql: "SELECT `id` FROM `test_model` WHERE ((`id`<?) OR (`id`>?)) AND (`first_name`>?);",
			wantArgs: []interface{}{13, 4, "tom"},
		},
		{
			name: "cross columns",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(C("Id").LT(13).Or(C("Age").GT(C("Id")))),
			wantSql: "SELECT `id` FROM `test_model` WHERE (`id`<?) OR (`age`>`id`);",
			wantArgs: []interface{}{13},
		},
		{
			name: "cross columns mathematical",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(C("Age").GT(C("Id").Add(40))),
			wantSql: "SELECT `id` FROM `test_model` WHERE `age`>(`id`+?);",
			wantArgs: []interface{}{40},
		},
		{
			name: "cross columns mathematical",
			builder: New().Select(Columns("Id")).From(&TestModel{Id: 10}).
				Where(C("Age").GT(C("Id").Multi(C("Age").Add(66)))),
			wantSql: "SELECT `id` FROM `test_model` WHERE `age`>(`id`*(`age`+?));",
			wantArgs: []interface{}{66},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			query, err := tc.builder.Build()
			assert.Equal(t, err, c.wantErr)
			if err != nil {
				return
			}
			assert.Equal(t, c.wantSql, query.SQL)
			assert.Equal(t, c.wantArgs, query.Args)
		})
	}
}

type TestModel struct {
	Id        int64 `eql:"auto_increment,primary_key"`
	FirstName string
	Age       int8
	LastName *string
}

type CommonTestCase struct {
	name string
	builder QueryBuilder
	wantArgs []interface{}
	wantSql string
	wantErr error
}

func stringPtr(val string) *string {
	return &val
}