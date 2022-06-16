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

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRawExpr_AsPredicate(t *testing.T) {
	db := memoryDB()
	testCases := []CommonTestCase{
		{
			name:     "simple",
			builder:  NewSelector[TestModel](db).From(&TestModel{}).Where(Raw("`id`<?", 12).AsPredicate()),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `id`<?;",
			wantArgs: []interface{}{12},
		},
		{
			name: "and",
			builder: NewSelector[TestModel](db).From(&TestModel{}).Where(Raw("`id`<?", 12).AsPredicate().
				And(Raw("`age`<?", 18).AsPredicate())),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE (`id`<?) AND (`age`<?);",
			wantArgs: []interface{}{12, 18},
		},
		{
			name: "Or",
			builder: NewSelector[TestModel](db).From(&TestModel{}).Where(Raw("`id`<?", 12).AsPredicate().
				Or(Raw("`age`<?", 18).AsPredicate())),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE (`id`<?) OR (`age`<?);",
			wantArgs: []interface{}{12, 18},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			query, err := c.builder.Build()
			assert.Equal(t, c.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, c.wantSql, query.SQL)
			assert.Equal(t, c.wantArgs, query.Args)
		})
	}
}

func ExampleRawExpr_AsPredicate() {
	pred := Raw("`id`<?", 12).AsPredicate()
	query, _ := NewSelector[TestModel](memoryDB()).From(&TestModel{}).Where(pred).Build()
	fmt.Println(query.string())
	// Output:
	// SQL: SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `id`<?;
	// Args: []interface {}{12}
}
