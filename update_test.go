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
	"github.com/gotomicro/eql/internal"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUpdater_Set(t *testing.T) {
	tm := &TestModel{
		Id:        12,
		FirstName: "Tom",
		Age:       18,
		LastName:  "Jerry",
	}
	testCases := []CommonTestCase {
		{
			name: "no set",
			builder: New().Update(tm),
			wantSql: "UPDATE `test_model` SET `id`=?,`first_name`=?,`age`=?,`last_name`=?;",
			wantArgs: []interface{}{int64(12), "Tom", int8(18), "Jerry"},
		},
		{
			name: "set columns",
			builder: New().Update(tm).Set(Columns("FirstName", "Age")),
			wantSql: "UPDATE `test_model` SET `first_name`=?,`age`=?;",
			wantArgs: []interface{}{"Tom", int8(18)},
		},
		{
			name: "set invalid columns",
			builder: New().Update(tm).Set(Columns("FirstNameInvalid", "Age")),
			wantErr: internal.NewInvalidColumnError("FirstNameInvalid"),
		},
		{
			name: "set c2",
			builder: New().Update(tm).Set(C("FirstName"), C("Age")),
			wantSql: "UPDATE `test_model` SET `first_name`=?,`age`=?;",
			wantArgs: []interface{}{"Tom", int8(18)},
		},

		{
			name: "set invalid c2",
			builder: New().Update(tm).Set(C("FirstNameInvalid"), C("Age")),
			wantErr: internal.NewInvalidColumnError("FirstNameInvalid"),
		},

		{
			name: "set assignment",
			builder: New().Update(tm).Set(C("FirstName"), Assign("Age", 30)),
			wantSql: "UPDATE `test_model` SET `first_name`=?,`age`=?;",
			wantArgs: []interface{}{"Tom", 30},
		},
		{
			name: "set invalid assignment",
			builder: New().Update(tm).Set(C("FirstName"), Assign("InvalidAge", 30)),
			wantErr: internal.NewInvalidColumnError("InvalidAge"),
		},
		{
			name: "set age+1",
			builder: New().Update(tm).Set(C("FirstName"), Assign("Age", C("Age").Add(1))),
			wantSql: "UPDATE `test_model` SET `first_name`=?,`age`=(`age`+?);",
			wantArgs: []interface{}{"Tom", 1},
		},
		{
			name: "set age=id+1",
			builder: New().Update(tm).Set(C("FirstName"), Assign("Age", C("Id").Add(10))),
			wantSql: "UPDATE `test_model` SET `first_name`=?,`age`=(`id`+?);",
			wantArgs: []interface{}{"Tom", 10},
		},
		{
			name: "set age=id+(age*100)",
			builder: New().Update(tm).Set(C("FirstName"), Assign("Age", C("Id").Add(C("Age").Multi(100)))),
			wantSql: "UPDATE `test_model` SET `first_name`=?,`age`=(`id`+(`age`*?));",
			wantArgs: []interface{}{"Tom", 100},
		},
		{
			name: "set age=(id+(age*100))*110",
			builder: New().Update(tm).Set(C("FirstName"), Assign("Age", C("Id").Add(C("Age").Multi(100)).Multi(110))),
			wantSql: "UPDATE `test_model` SET `first_name`=?,`age`=((`id`+(`age`*?))*?);",
			wantArgs: []interface{}{"Tom", 100, 110},
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
