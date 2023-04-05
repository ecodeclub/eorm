// Copyright 2021 ecodeclub
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
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPredicate_C(t *testing.T) {
	db := memoryDB()
	testCases := []CommonTestCase{
		{
			name:    "empty",
			builder: NewSelector[TestModel](db).Select(Columns("Id")).Where(),
			wantSql: "SELECT `id` FROM `test_model`;",
		},
		{
			name: "multiples",
			// 在传入多个 Predicate 的时候，我们认为它们是用 and 连接起来的
			builder: NewSelector[TestModel](db).Select(Columns("Id")).
				Where(C("Id").LT(13), C("Id").GT(4)),
			wantSql:  "SELECT `id` FROM `test_model` WHERE (`id`<?) AND (`id`>?);",
			wantArgs: []interface{}{13, 4},
		},
		{
			name: "and",
			builder: NewSelector[TestModel](db).Select(Columns("Id")).
				Where(C("Id").LT(13).And(C("Id").GT(4))),
			wantSql:  "SELECT `id` FROM `test_model` WHERE (`id`<?) AND (`id`>?);",
			wantArgs: []interface{}{13, 4},
		},
		{
			name: "or",
			builder: NewSelector[TestModel](db).Select(Columns("Id")).
				Where(C("Id").LT(13).Or(C("Id").GT(4))),
			wantSql:  "SELECT `id` FROM `test_model` WHERE (`id`<?) OR (`id`>?);",
			wantArgs: []interface{}{13, 4},
		},
		{
			name: "not",
			builder: NewSelector[TestModel](db).Select(Columns("Id")).
				Where(Not(C("Id").LT(13).Or(C("Id").GT(4)))),
			wantSql:  "SELECT `id` FROM `test_model` WHERE NOT ((`id`<?) OR (`id`>?));",
			wantArgs: []interface{}{13, 4},
		},
		{
			name: "and or",
			builder: NewSelector[TestModel](db).Select(Columns("Id")).
				Where(C("Id").LT(13).Or(C("Id").GT(4)).And(C("FirstName").GT("tom"))),
			wantSql:  "SELECT `id` FROM `test_model` WHERE ((`id`<?) OR (`id`>?)) AND (`first_name`>?);",
			wantArgs: []interface{}{13, 4, "tom"},
		},
		{
			name: "cross columns",
			builder: NewSelector[TestModel](db).Select(Columns("Id")).
				Where(C("Id").LT(13).Or(C("Age").GT(C("Id")))),
			wantSql:  "SELECT `id` FROM `test_model` WHERE (`id`<?) OR (`age`>`id`);",
			wantArgs: []interface{}{13},
		},
		{
			name: "cross columns mathematical",
			builder: NewSelector[TestModel](db).Select(Columns("Id")).
				Where(C("Age").GT(C("Id").Add(40))),
			wantSql:  "SELECT `id` FROM `test_model` WHERE `age`>(`id`+?);",
			wantArgs: []interface{}{40},
		},
		{
			name: "cross columns mathematical",
			builder: NewSelector[TestModel](db).Select(Columns("Id")).
				Where(C("Age").GT(C("Id").Multi(C("Age").Add(66)))),
			wantSql:  "SELECT `id` FROM `test_model` WHERE `age`>(`id`*(`age`+?));",
			wantArgs: []interface{}{66},
		},
		{
			name:     "Avg with EQ",
			builder:  NewSelector[TestModel](db).Select().GroupBy("FirstName").Having(Avg("Age").EQ(18)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name` HAVING AVG(`age`)=?;",
			wantArgs: []interface{}{18},
		},
		{
			name:     "Max with NEQ",
			builder:  NewSelector[TestModel](db).Select().GroupBy("FirstName").Having(Max("Age").NEQ(18)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name` HAVING MAX(`age`)!=?;",
			wantArgs: []interface{}{18},
		},
		{
			name:     "Min with LT",
			builder:  NewSelector[TestModel](db).Select().GroupBy("FirstName").Having(Min("Age").LT(18)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name` HAVING MIN(`age`)<?;",
			wantArgs: []interface{}{18},
		},
		{
			name:     "Sum with LTEQ",
			builder:  NewSelector[TestModel](db).Select().GroupBy("FirstName").Having(Sum("Age").LTEQ(18)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name` HAVING SUM(`age`)<=?;",
			wantArgs: []interface{}{18},
		},
		{
			name:     "Count with GT",
			builder:  NewSelector[TestModel](db).Select().GroupBy("FirstName").Having(Count("Age").GT(18)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name` HAVING COUNT(`age`)>?;",
			wantArgs: []interface{}{18},
		},
		{
			name:     "Avg with GTEQ",
			builder:  NewSelector[TestModel](db).Select().GroupBy("FirstName").Having(Avg("Age").GTEQ(18)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name` HAVING AVG(`age`)>=?;",
			wantArgs: []interface{}{18},
		},
		{
			name:     "multiples aggregate functions",
			builder:  NewSelector[TestModel](db).Select().GroupBy("FirstName").Having(Avg("Age").GTEQ(18).And(Sum("Age").LTEQ(30))),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name` HAVING (AVG(`age`)>=?) AND (SUM(`age`)<=?);",
			wantArgs: []interface{}{18, 30},
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
	Id        int64 `eorm:"auto_increment,primary_key"`
	FirstName string
	Age       int8
	LastName  *sql.NullString
}

func (TestModel) CreateSQL() string {
	return `
CREATE TABLE IF NOT EXISTS test_model(
    id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    age INTEGER,
    last_name TEXT NOT NULL
)
`
}

type CommonTestCase struct {
	name     string
	builder  QueryBuilder
	wantArgs []interface{}
	wantSql  string
	wantErr  error
}

func ExampleNot() {
	db := memoryDB()
	query, _ := NewSelector[TestModel](db).Select(Columns("Id")).Where(Not(C("Id").EQ(18))).Build()
	fmt.Println(query.String())
	// Output:
	// SQL: SELECT `id` FROM `test_model` WHERE NOT (`id`=?);
	// Args: []interface {}{18}
}

func ExamplePredicate_And() {
	db := memoryDB()
	query, _ := NewSelector[TestModel](db).Select(Columns("Id")).Where(C("Id").EQ(18).And(C("Age").GT(100))).Build()
	fmt.Println(query.String())
	// Output:
	// SQL: SELECT `id` FROM `test_model` WHERE (`id`=?) AND (`age`>?);
	// Args: []interface {}{18, 100}
}

func ExamplePredicate_Or() {
	db := memoryDB()
	query, _ := NewSelector[TestModel](db).Select(Columns("Id")).Where(C("Id").EQ(18).Or(C("Age").GT(100))).Build()
	fmt.Println(query.String())
	// Output:
	// SQL: SELECT `id` FROM `test_model` WHERE (`id`=?) OR (`age`>?);
	// Args: []interface {}{18, 100}
}
