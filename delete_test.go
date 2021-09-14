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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleter_Build(t *testing.T) {
	testCases := []CommonTestCase{
		{
			name:    "no where",
			builder: New().Delete().From(&TestModel{}),
			wantSql: "DELETE FROM `test_model`;",
		},
		//{
		//	name:     "where",
		//	builder:  New().Delete().From(&TestModel{Id: 14}).Where(P("Id")),
		//	wantSql:  "DELETE FROM `test_model` WHERE `id`=?;",
		//	wantArgs: []interface{}{int64(14)},
		//},
		//{
		//	name: "order",
		//	builder: New().Delete().From(&TestModel{Id: 14}).Where(P("Id")).
		//		OrderBy(ASC("Id"), DESC("Name")),
		//	wantSql:  "DELETE FROM `test_model` WHERE `id`=? ORDER BY `id` ASC, `name` DESC;",
		//	wantArgs: []interface{}{int64(14)},
		//},
		//{
		//	name: "order and limit",
		//	builder: New().Delete().From(&TestModel{Id: 14}).Where(P("Id")).
		//		OrderBy(ASC("Id"), DESC("Name")).Limit(3),
		//	wantSql:  "DELETE FROM `test_model` WHERE `id`=? ORDER BY `id` ASC, `name` DESC LIMIT ?;",
		//	wantArgs: []interface{}{int64(14), 3},
		//},
		//
		//{
		//	name:     "limit",
		//	builder:  New().Delete().From(&TestModel{Id: 14}).Where(P("Id")).Limit(3),
		//	wantSql:  "DELETE FROM `test_model` WHERE `id`=? LIMIT ?;",
		//	wantArgs: []interface{}{int64(14), 3},
		//},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			query, err := c.builder.Build()
			fmt.Println(query.SQL)
			assert.Equal(t, c.wantErr, err)
			assert.Equal(t, c.wantSql, query.SQL)
			assert.Equal(t, c.wantArgs, query.Args)
		})
	}
}
