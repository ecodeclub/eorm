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

func TestAggregate(t *testing.T) {
	testCases := []CommonTestCase{
		{
			name:    "avg",
			builder: New().Select(Avg("Age")).From(&TestModel{}),
			wantSql: "SELECT AVG(`age`) FROM `test_model`;",
		},
		{
			name:    "max",
			builder: New().Select(Max("Age")).From(&TestModel{}),
			wantSql: "SELECT MAX(`age`) FROM `test_model`;",
		},
		{
			name:    "min",
			builder: New().Select(Min("Age").As("min_age")).From(&TestModel{}),
			wantSql: "SELECT MIN(`age`) AS `min_age` FROM `test_model`;",
		},
		{
			name:    "sum",
			builder: New().Select(Sum("Age")).From(&TestModel{}),
			wantSql: "SELECT SUM(`age`) FROM `test_model`;",
		},
		{
			name:    "count",
			builder: New().Select(Count("Age")).From(&TestModel{}),
			wantSql: "SELECT COUNT(`age`) FROM `test_model`;",
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
	query, _ := New().Select(Avg("Age").As("avg_age")).From(&TestModel{}).Build()
	fmt.Println(query.SQL)
	// Output: SELECT AVG(`age`) AS `avg_age` FROM `test_model`;
}

func ExampleAvg() {
	query, _ := New().Select(Avg("Age").As("avg_age")).From(&TestModel{}).Build()
	fmt.Println(query.SQL)
	// Output: SELECT AVG(`age`) AS `avg_age` FROM `test_model`;
}

func ExampleCount() {
	query, _ := New().Select(Count("Age")).From(&TestModel{}).Build()
	fmt.Println(query.SQL)
	// Output: SELECT COUNT(`age`) FROM `test_model`;
}

func ExampleMax() {
	query, _ := New().Select(Max("Age")).From(&TestModel{}).Build()
	fmt.Println(query.SQL)
	// Output: SELECT MAX(`age`) FROM `test_model`;
}

func ExampleMin() {
	query, _ := New().Select(Min("Age")).From(&TestModel{}).Build()
	fmt.Println(query.SQL)
	// Output: SELECT MIN(`age`) FROM `test_model`;
}

func ExampleSum() {
	query, _ := New().Select(Sum("Age")).From(&TestModel{}).Build()
	fmt.Println(query.SQL)
	// Output: SELECT SUM(`age`) FROM `test_model`;
}
