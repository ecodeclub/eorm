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

func TestDeleter_Build(t *testing.T) {
	testCases := []CommonTestCase{
		{
			name:    "no where",
			builder: memoryDB().Delete().From(&TestModel{}),
			wantSql: "DELETE FROM `test_model`;",
		},
		{
			name:     "where",
			builder:  memoryDB().Delete().From(&TestModel{}).Where(C("Id").EQ(16)),
			wantSql:  "DELETE FROM `test_model` WHERE `id`=?;",
			wantArgs: []interface{}{16},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			query, err := c.builder.Build()
			assert.Equal(t, c.wantErr, err)
			assert.Equal(t, c.wantSql, query.SQL)
			assert.Equal(t, c.wantArgs, query.Args)
		})
	}
}

func ExampleDeleter_Build() {
	query, _ := memoryDB().Delete().From(&TestModel{}).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: DELETE FROM `test_model`;
}

func ExampleDeleter_From() {
	query, _ := memoryDB().Delete().From(&TestModel{}).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: DELETE FROM `test_model`;
}

func ExampleDeleter_Where() {
	query, _ := memoryDB().Delete().From(&TestModel{}).Where(C("Id").EQ(12)).Build()
	fmt.Printf("SQL: %s\nArgs: %v", query.SQL, query.Args)
	// Output:
	// SQL: DELETE FROM `test_model` WHERE `id`=?;
	// Args: [12]
}
