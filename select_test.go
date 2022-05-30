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
	"github.com/gotomicro/eorm/internal/errs"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelectable(t *testing.T) {
	db := NewDB()
	testCases := []CommonTestCase{
		{
			name:    "simple",
			builder: NewSelector(db).From(&TestModel{}),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model`;",
		},
		{
			name:    "columns",
			builder: NewSelector(db).Select(Columns("Id", "FirstName")).From(&TestModel{}),
			wantSql: "SELECT `id`,`first_name` FROM `test_model`;",
		},
		{
			name:    "alias",
			builder: NewSelector(db).Select(Columns("Id"), C("FirstName").As("name")).From(&TestModel{}),
			wantSql: "SELECT `id`,`first_name` AS `name` FROM `test_model`;",
		},
		{
			name:    "aggregate",
			builder: NewSelector(db).Select(Columns("Id"), Avg("Age").As("avg_age")).From(&TestModel{}),
			wantSql: "SELECT `id`,AVG(`age`) AS `avg_age` FROM `test_model`;",
		},
		{
			name:    "raw",
			builder: NewSelector(db).Select(Columns("Id"), Raw("AVG(DISTINCT `age`)")).From(&TestModel{}),
			wantSql: "SELECT `id`,AVG(DISTINCT `age`) FROM `test_model`;",
		},
		{
			name:    "invalid columns",
			builder: NewSelector(db).Select(Columns("Invalid"), Raw("AVG(DISTINCT `age`)")).From(&TestModel{}),
			wantErr: errs.NewInvalidColumnError("Invalid"),
		},
		{
			name:    "order by",
			builder: NewSelector(db).From(&TestModel{}).OrderBy(ASC("Age"), DESC("Id")),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC;",
		},
		{
			name:    "order by invalid column",
			builder: NewSelector(db).From(&TestModel{}).OrderBy(ASC("Invalid"), DESC("Id")),
			wantErr: errs.NewInvalidColumnError("Invalid"),
		},
		{
			name:    "group by",
			builder: NewSelector(db).From(&TestModel{}).GroupBy("Age", "Id"),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `age`,`id`;",
		},
		{
			name:    "group by invalid column",
			builder: NewSelector(db).From(&TestModel{}).GroupBy("Invalid", "Id"),
			wantErr: errs.NewInvalidColumnError("Invalid"),
		},
		{
			name:     "offset",
			builder:  NewSelector(db).From(&TestModel{}).OrderBy(ASC("Age"), DESC("Id")).Offset(10),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC OFFSET ?;",
			wantArgs: []interface{}{10},
		},
		{
			name:     "limit",
			builder:  NewSelector(db).From(&TestModel{}).OrderBy(ASC("Age"), DESC("Id")).Offset(10).Limit(100),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC OFFSET ? LIMIT ?;",
			wantArgs: []interface{}{10, 100},
		},
		{
			name:     "where",
			builder:  NewSelector(db).From(&TestModel{}).Where(C("Id").EQ(10)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `id`=?;",
			wantArgs: []interface{}{10},
		},
		{
			name:    "no where",
			builder: NewSelector(db).From(&TestModel{}).Where(),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model`;",
		},
		{
			name:     "having",
			builder:  NewSelector(db).From(&TestModel{}).GroupBy("FirstName").Having(Avg("Age").EQ(18)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name` HAVING AVG(`age`)=?;",
			wantArgs: []interface{}{18},
		},
		{
			name:    "no having",
			builder: NewSelector(db).From(&TestModel{}).GroupBy("FirstName").Having(),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name`;",
		},
		{
			name:     "alias in having",
			builder:  NewSelector(db).Select(Columns("Id"), Columns("FirstName"), Avg("Age").As("avg_age")).From(&TestModel{}).GroupBy("FirstName").Having(C("avg_age").LT(20)),
			wantSql:  "SELECT `id`,`first_name`,AVG(`age`) AS `avg_age` FROM `test_model` GROUP BY `first_name` HAVING `avg_age`<?;",
			wantArgs: []interface{}{20},
		},
		{
			name:    "invalid alias in having",
			builder: NewSelector(db).Select(Columns("Id"), Columns("FirstName"), Avg("Age").As("avg_age")).From(&TestModel{}).GroupBy("FirstName").Having(C("Invalid").LT(20)),
			wantErr: errs.NewInvalidColumnError("Invalid"),
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
	db := NewDB()
	query, _ := NewSelector(db).From(&TestModel{}).OrderBy(ASC("Age")).Build()
	fmt.Printf("case1\n%s", query.string())
	query, _ = NewSelector(db).From(&TestModel{}).OrderBy(ASC("Age", "Id")).Build()
	fmt.Printf("case2\n%s", query.string())
	query, _ = NewSelector(db).From(&TestModel{}).OrderBy(ASC("Age"), ASC("Id")).Build()
	fmt.Printf("case3\n%s", query.string())
	query, _ = NewSelector(db).From(&TestModel{}).OrderBy(ASC("Age"), DESC("Id")).Build()
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

func ExampleSelector_Having() {
	db := NewDB()
	query, _ := NewSelector(db).Select(Columns("Id"), Columns("FirstName"), Avg("Age").As("avg_age")).From(&TestModel{}).GroupBy("FirstName").Having(C("avg_age").LT(20)).Build()
	fmt.Printf("case1\n%s", query.string())
	query, err := NewSelector(db).Select(Columns("Id"), Columns("FirstName"), Avg("Age").As("avg_age")).From(&TestModel{}).GroupBy("FirstName").Having(C("Invalid").LT(20)).Build()
	fmt.Printf("case2\n%s", err)
	// Output:
	// case1
	// SQL: SELECT `id`,`first_name`,AVG(`age`) AS `avg_age` FROM `test_model` GROUP BY `first_name` HAVING `avg_age`<?;
	// Args: []interface {}{20}
	// case2
	// eorm: 未知字段 Invalid

}
