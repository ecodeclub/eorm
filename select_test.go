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
	"github.com/gotomicro/eql/internal"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelectable(t *testing.T) {
	testCases := []CommonTestCase{
		{
			name:    "simple",
			builder: New().Select().From(&TestModel{}),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model`;",
		},
		{
			name:    "columns",
			builder: New().Select(Columns("Id", "FirstName")).From(&TestModel{}),
			wantSql: "SELECT `id`,`first_name` FROM `test_model`;",
		},
		{
			name:    "alias",
			builder: New().Select(Columns("Id"), C("FirstName").As("name")).From(&TestModel{}),
			wantSql: "SELECT `id`,`first_name` AS `name` FROM `test_model`;",
		},
		{
			name:    "aggregate",
			builder: New().Select(Columns("Id"), Avg("Age").As("avg_age")).From(&TestModel{}),
			wantSql: "SELECT `id`,AVG(`age`) AS `avg_age` FROM `test_model`;",
		},
		{
			name:    "raw",
			builder: New().Select(Columns("Id"), Raw("AVG(DISTINCT `age`)")).From(&TestModel{}),
			wantSql: "SELECT `id`,AVG(DISTINCT `age`) FROM `test_model`;",
		},
		{
			name:    "invalid columns",
			builder: New().Select(Columns("Invalid"), Raw("AVG(DISTINCT `age`)")).From(&TestModel{}),
			wantErr: internal.NewInvalidColumnError("Invalid"),
		},
		{
			name:    "order by",
			builder: New().Select().From(&TestModel{}).OrderBy(ASC("Age"), DESC("Id")),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC;",
		},
		{
			name:    "order by invalid column",
			builder: New().Select().From(&TestModel{}).OrderBy(ASC("Invalid"), DESC("Id")),
			wantErr: internal.NewInvalidColumnError("Invalid"),
		},
		{
			name:    "group by",
			builder: New().Select().From(&TestModel{}).GroupBy("Age", "Id"),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `age`,`id`;",
		},
		{
			name:    "group by invalid column",
			builder: New().Select().From(&TestModel{}).GroupBy("Invalid", "Id"),
			wantErr: internal.NewInvalidColumnError("Invalid"),
		},
		{
			name:     "offset",
			builder:  New().Select().From(&TestModel{}).OrderBy(ASC("Age"), DESC("Id")).Offset(10),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC OFFSET ?;",
			wantArgs: []interface{}{10},
		},
		{
			name:     "limit",
			builder:  New().Select().From(&TestModel{}).OrderBy(ASC("Age"), DESC("Id")).Offset(10).Limit(100),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC OFFSET ? LIMIT ?;",
			wantArgs: []interface{}{10, 100},
		},
		{
			name:     "where",
			builder:  New().Select().From(&TestModel{}).Where(C("Id").EQ(10)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `id`=?;",
			wantArgs: []interface{}{10},
		},
		{
			name:    "no where",
			builder: New().Select().From(&TestModel{}).Where(),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model`;",
		},
		{
			name:     "having",
			builder:  New().Select().From(&TestModel{}).GroupBy("FirstName").Having(Avg("Age").EQ(18)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name` HAVING AVG(`age`)=?;",
			wantArgs: []interface{}{18},
		},
		{
			name:    "no having",
			builder: New().Select().From(&TestModel{}).GroupBy("FirstName").Having(),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name`;",
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

func ExampleSelector_OrderBy() {
	query, _ := New().Select().From(&TestModel{}).OrderBy(ASC("Age")).Build()
	fmt.Printf("case1\n%s", query.string())
	query, _ = New().Select().From(&TestModel{}).OrderBy(ASC("Age", "Id")).Build()
	fmt.Printf("case2\n%s", query.string())
	query, _ = New().Select().From(&TestModel{}).OrderBy(ASC("Age"), ASC("Id")).Build()
	fmt.Printf("case3\n%s", query.string())
	query, _ = New().Select().From(&TestModel{}).OrderBy(ASC("Age"), DESC("Id")).Build()
	fmt.Printf("case4\n%s", query.string())
	// Output:
	// case1
	// SQL: SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC;
	// Args: []interface {}(nil)
	// case2
	// SQL: SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age``id` ASC;
	// Args: []interface {}(nil)
	// case3
	// SQL: SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` ASC;
	// Args: []interface {}(nil)
	// case4
	// SQL: SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC;
	// Args: []interface {}(nil)
}
