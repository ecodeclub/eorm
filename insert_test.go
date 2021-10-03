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
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestInserter_Values(t *testing.T) {
	type User struct {
		Id        int64
		FirstName string
		Ctime     time.Time
	}
	type Order struct {
		Id    int64
		Name  string
		Price int64
	}

	n := time.Now()
	u := &User{
		Id:        12,
		FirstName: "Tom",
		Ctime:     n,
	}
	u1 := &User{
		Id:        13,
		FirstName: "Jerry",
		Ctime:     n,
	}
	o1 := &Order{
		Id:    14,
		Name:  "Hellen",
		Price: 200,
	}
	testCases := []CommonTestCase{
		{
			name:    "no values",
			builder: New().Insert().Values(),
			wantErr: errors.New("no values"),
		},
		{
			name:    "single value",
			builder: New().Insert().Values(u),
			// 这个列的顺序不重要，但是列和 Args 的位置要对应得上
			wantSql:  "INSERT INTO `user`(`id`,`first_name`,`ctime`) VALUES(?,?,?);",
			wantArgs: []interface{}{int64(12), "Tom", n},
		},

		{
			name:    "multiple values",
			builder: New().Insert().Values(u, u1),
			// 这个列的顺序不重要，但是列和 Args 的位置要对应得上
			wantSql:  "INSERT INTO `user`(`id`,`first_name`,`ctime`) VALUES(?,?,?),(?,?,?);",
			wantArgs: []interface{}{int64(12), "Tom", n, int64(13), "Jerry", n},
		},

		{
			name:     "no whole columns", //字段名
			builder:  New().Insert().Columns("Id", "FirstName").Values(u),
			wantSql:  "INSERT INTO `user`(`id`,`first_name`) VALUES(?,?);",
			wantArgs: []interface{}{int64(12), "Tom"},
		},
		{
			name:    "error columns",
			builder: New().Insert().Columns("id", "FirstName").Values(u),
			wantErr: errors.New("error columns"),
		},
		{
			name:     "no whole columns but multiple values",
			builder:  New().Insert().Columns("Id", "FirstName").Values(u, u1),
			wantSql:  "INSERT INTO `user`(`id`,`first_name`) VALUES(?,?),(?,?);",
			wantArgs: []interface{}{int64(12), "Tom", int64(13), "Jerry"},
		},
		{
			name:    "multiple values of different type",
			builder: New().Insert().Values(u, o1),
			wantErr: errors.New("multiple values of different type"),
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(tc.name, func(t *testing.T) {
			q, err := c.builder.Build()
			assert.Equal(t, c.wantErr, err)
			assert.Equal(t, c.wantSql, q.SQL)
			assert.Equal(t, c.wantArgs, q.Args)
		})
	}
}
