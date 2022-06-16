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
	db := memoryDB()
	testCases := []CommonTestCase{
		{
			name:    "simple",
			builder: NewSelector[TestModel](db).From(&TestModel{}),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model`;",
		},
		{
			name:    "columns",
			builder: NewSelector[TestModel](db).Select(Columns("Id", "FirstName")).From(&TestModel{}),
			wantSql: "SELECT `id`,`first_name` FROM `test_model`;",
		},
		{
			name:    "alias",
			builder: NewSelector[TestModel](db).Select(Columns("Id"), C("FirstName").As("name")).From(&TestModel{}),
			wantSql: "SELECT `id`,`first_name` AS `name` FROM `test_model`;",
		},
		{
			name:    "aggregate",
			builder: NewSelector[TestModel](db).Select(Columns("Id"), Avg("Age").As("avg_age")).From(&TestModel{}),
			wantSql: "SELECT `id`,AVG(`age`) AS `avg_age` FROM `test_model`;",
		},
		{
			name:    "raw",
			builder: NewSelector[TestModel](db).Select(Columns("Id"), Raw("AVG(DISTINCT `age`)")).From(&TestModel{}),
			wantSql: "SELECT `id`,AVG(DISTINCT `age`) FROM `test_model`;",
		},
		{
			name:    "invalid columns",
			builder: NewSelector[TestModel](db).Select(Columns("Invalid"), Raw("AVG(DISTINCT `age`)")).From(&TestModel{}),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name:    "order by",
			builder: NewSelector[TestModel](db).From(&TestModel{}).OrderBy(ASC("Age"), DESC("Id")),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC;",
		},
		{
			name:    "order by invalid column",
			builder: NewSelector[TestModel](db).From(&TestModel{}).OrderBy(ASC("Invalid"), DESC("Id")),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name:    "group by",
			builder: NewSelector[TestModel](db).From(&TestModel{}).GroupBy("Age", "Id"),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `age`,`id`;",
		},
		{
			name:    "group by invalid column",
			builder: NewSelector[TestModel](db).From(&TestModel{}).GroupBy("Invalid", "Id"),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name:     "offset",
			builder:  NewSelector[TestModel](db).From(&TestModel{}).OrderBy(ASC("Age"), DESC("Id")).Offset(10),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC OFFSET ?;",
			wantArgs: []interface{}{10},
		},
		{
			name:     "limit",
			builder:  NewSelector[TestModel](db).From(&TestModel{}).OrderBy(ASC("Age"), DESC("Id")).Offset(10).Limit(100),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC OFFSET ? LIMIT ?;",
			wantArgs: []interface{}{10, 100},
		},
		{
			name:     "where",
			builder:  NewSelector[TestModel](db).From(&TestModel{}).Where(C("Id").EQ(10)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `id`=?;",
			wantArgs: []interface{}{10},
		},
		{
			name:    "no where",
			builder: NewSelector[TestModel](db).From(&TestModel{}).Where(),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model`;",
		},
		{
			name:     "having",
			builder:  NewSelector[TestModel](db).From(&TestModel{}).GroupBy("FirstName").Having(Avg("Age").EQ(18)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name` HAVING AVG(`age`)=?;",
			wantArgs: []interface{}{18},
		},
		{
			name:    "no having",
			builder: NewSelector[TestModel](db).From(&TestModel{}).GroupBy("FirstName").Having(),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name`;",
		},
		{
			name:     "alias in having",
			builder:  NewSelector[TestModel](db).Select(Columns("Id"), Columns("FirstName"), Avg("Age").As("avg_age")).From(&TestModel{}).GroupBy("FirstName").Having(C("avg_age").LT(20)),
			wantSql:  "SELECT `id`,`first_name`,AVG(`age`) AS `avg_age` FROM `test_model` GROUP BY `first_name` HAVING `avg_age`<?;",
			wantArgs: []interface{}{20},
		},
		{
			name:    "invalid alias in having",
			builder: NewSelector[TestModel](db).Select(Columns("Id"), Columns("FirstName"), Avg("Age").As("avg_age")).From(&TestModel{}).GroupBy("FirstName").Having(C("Invalid").LT(20)),
			wantErr: errs.NewInvalidFieldError("Invalid"),
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
	db := memoryDB()
	query, _ := NewSelector[TestModel](db).From(&TestModel{}).OrderBy(ASC("Age")).Build()
	fmt.Printf("case1\n%s", query.string())
	query, _ = NewSelector[TestModel](db).From(&TestModel{}).OrderBy(ASC("Age", "Id")).Build()
	fmt.Printf("case2\n%s", query.string())
	query, _ = NewSelector[TestModel](db).From(&TestModel{}).OrderBy(ASC("Age"), ASC("Id")).Build()
	fmt.Printf("case3\n%s", query.string())
	query, _ = NewSelector[TestModel](db).From(&TestModel{}).OrderBy(ASC("Age"), DESC("Id")).Build()
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
	db := memoryDB()
	query, _ := NewSelector[TestModel](db).Select(Columns("Id"), Columns("FirstName"), Avg("Age").As("avg_age")).From(&TestModel{}).GroupBy("FirstName").Having(C("avg_age").LT(20)).Build()
	fmt.Printf("case1\n%s", query.string())
	query, err := NewSelector[TestModel](db).Select(Columns("Id"), Columns("FirstName"), Avg("Age").As("avg_age")).From(&TestModel{}).GroupBy("FirstName").Having(C("Invalid").LT(20)).Build()
	fmt.Printf("case2\n%s", err)
	// Output:
	// case1
	// SQL: SELECT `id`,`first_name`,AVG(`age`) AS `avg_age` FROM `test_model` GROUP BY `first_name` HAVING `avg_age`<?;
	// Args: []interface {}{20}
	// case2
	// eorm: 未知字段 Invalid
}

func ExampleSelector_Select() {
	db := memoryDB()
	tm := &TestModel{}
	cases := []*Selector[TestModel]{
		// case0: all columns are included
		NewSelector[TestModel](db).From(tm),
		// case1: only query specific columns
		NewSelector[TestModel](db).Select(Columns("Id", "Age")).From(tm),
		// case2: using alias
		NewSelector[TestModel](db).Select(C("Id").As("my_id")).From(tm),
		// case3: using aggregation function and alias
		NewSelector[TestModel](db).Select(Avg("Age").As("avg_age")).From(tm),
		// case4: using raw expression
		NewSelector[TestModel](db).Select(Raw("COUNT(DISTINCT `age`) AS `age_cnt`")).From(tm),
	}

	for index, tc := range cases {
		query, _ := tc.Build()
		fmt.Printf("case%d:\n%s", index, query.string())
	}
	// Output:
	// case0:
	// SQL: SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model`;
	// Args: []interface {}(nil)
	// case1:
	// SQL: SELECT `id`,`age` FROM `test_model`;
	// Args: []interface {}(nil)
	// case2:
	// SQL: SELECT `id` AS `my_id` FROM `test_model`;
	// Args: []interface {}(nil)
	// case3:
	// SQL: SELECT AVG(`age`) AS `avg_age` FROM `test_model`;
	// Args: []interface {}(nil)
	// case4:
	// SQL: SELECT COUNT(DISTINCT `age`) AS `age_cnt` FROM `test_model`;
	// Args: []interface {}(nil)
}
