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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAggregate(t *testing.T) {
	db := memoryDB()
	testCases := []CommonTestCase{
		{
			name:    "avg",
			builder: NewSelector[TestModel](db).Select(Avg("Age")).From(TableOf(&TestModel{})),
			wantSql: "SELECT AVG(`age`) FROM `test_model`;",
		},
		{
			name:    "max",
			builder: NewSelector[TestModel](db).Select(Max("Age")).From(TableOf(&TestModel{})),
			wantSql: "SELECT MAX(`age`) FROM `test_model`;",
		},
		{
			name:    "min",
			builder: NewSelector[TestModel](db).Select(Min("Age").As("min_age")).From(TableOf(&TestModel{})),
			wantSql: "SELECT MIN(`age`) AS `min_age` FROM `test_model`;",
		},
		{
			name:    "sum",
			builder: NewSelector[TestModel](db).Select(Sum("Age")).From(TableOf(&TestModel{})),
			wantSql: "SELECT SUM(`age`) FROM `test_model`;",
		},
		{
			name:    "count",
			builder: NewSelector[TestModel](db).Select(Count("Age")).From(TableOf(&TestModel{})),
			wantSql: "SELECT COUNT(`age`) FROM `test_model`;",
		},
		{
			name:    "count distinct",
			builder: NewSelector[TestModel](db).From(TableOf(&TestModel{})).Select(CountDistinct("FirstName")),
			wantSql: "SELECT COUNT(DISTINCT `first_name`) FROM `test_model`;",
		},
		{
			name:    "avg distinct",
			builder: NewSelector[TestModel](db).From(TableOf(&TestModel{})).Select(AvgDistinct("FirstName")),
			wantSql: "SELECT AVG(DISTINCT `first_name`) FROM `test_model`;",
		},
		{
			name:    "SUM distinct",
			builder: NewSelector[TestModel](db).From(TableOf(&TestModel{})).Select(SumDistinct("FirstName")),
			wantSql: "SELECT SUM(DISTINCT `first_name`) FROM `test_model`;",
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

func ExampleAggregate_As() {
	db := memoryDB()
	query, _ := NewSelector[TestModel](db).Select(Avg("Age").As("avg_age")).From(TableOf(&TestModel{})).Build()
	fmt.Println(query.SQL)
	// Output: SELECT AVG(`age`) AS `avg_age` FROM `test_model`;
}

func ExampleAvg() {
	db := memoryDB()
	query, _ := NewSelector[TestModel](db).Select(Avg("Age").As("avg_age")).From(TableOf(&TestModel{})).Build()
	fmt.Println(query.SQL)
	// Output: SELECT AVG(`age`) AS `avg_age` FROM `test_model`;
}

func ExampleCount() {
	db := memoryDB()
	query, _ := NewSelector[TestModel](db).Select(Count("Age")).From(TableOf(&TestModel{})).Build()
	fmt.Println(query.SQL)
	// Output: SELECT COUNT(`age`) FROM `test_model`;
}

func ExampleMax() {
	db := memoryDB()
	query, _ := NewSelector[TestModel](db).Select(Max("Age")).From(TableOf(&TestModel{})).Build()
	fmt.Println(query.SQL)
	// Output: SELECT MAX(`age`) FROM `test_model`;
}

func ExampleMin() {
	db := memoryDB()
	query, _ := NewSelector[TestModel](db).Select(Min("Age")).From(TableOf(&TestModel{})).Build()
	fmt.Println(query.SQL)
	// Output: SELECT MIN(`age`) FROM `test_model`;
}

func ExampleSum() {
	db := memoryDB()
	query, _ := NewSelector[TestModel](db).Select(Sum("Age")).From(TableOf(&TestModel{})).Build()
	fmt.Println(query.SQL)
	// Output: SELECT SUM(`age`) FROM `test_model`;
}
