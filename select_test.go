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

func TestSelectable(t *testing.T) {
	testCases := []CommonTestCase{
		{
			name: "simple",
			builder: New().Select().From(&TestModel{}),
			wantSql: "SELECT `id`, `first_name`, `age`, `last_name` FROM `test_model`;",
		},
		{
			name: "columns",
			builder: New().Select(Columns("Id", "FirstName")).From(&TestModel{}),
			wantSql: "SELECT `id`, `first_name` FROM `test_model`;",
		},
		{
			name: "alias",
			builder: New().Select(Columns("Id"), C("FirstName").As("name")).From(&TestModel{}),
			wantSql: "SELECT `id`, `first_name` as `name` FROM `test_model`;",
		},
		{
			name: "aggregate",
			builder: New().Select(Columns("Id"), Avg("Age").As("avg_age")).From(&TestModel{}),
			wantSql: "SELECT `id`, AVG(`age`) as `avg_age` FROM `test_model`;",
		},
		{
			name: "raw",
			builder: New().Select(Columns("Id"), Raw("AVG(DISTINCT `age`)")).From(&TestModel{}),
			wantSql: "SELECT `id`, AVG(DISTINCT `age`) FROM `test_model`;",
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