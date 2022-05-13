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
	"errors"
	"fmt"
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
			name:    "no examples of values",
			builder: NewDB().Insert().Values(),
			wantErr: errors.New("no values"),
		},
		{
			name:     "single example of values",
			builder:  NewDB().Insert().Values(u),
			wantSql:  "INSERT INTO `user`(`id`,`first_name`,`ctime`) VALUES(?,?,?);",
			wantArgs: []interface{}{int64(12), "Tom", n},
		},

		{
			name:     "multiple values of same type",
			builder:  NewDB().Insert().Values(u, u1),
			wantSql:  "INSERT INTO `user`(`id`,`first_name`,`ctime`) VALUES(?,?,?),(?,?,?);",
			wantArgs: []interface{}{int64(12), "Tom", n, int64(13), "Jerry", n},
		},

		{
			name:     "no example of a whole columns",
			builder:  NewDB().Insert().Columns("Id", "FirstName").Values(u),
			wantSql:  "INSERT INTO `user`(`id`,`first_name`) VALUES(?,?);",
			wantArgs: []interface{}{int64(12), "Tom"},
		},
		{
			name:    "an example with invalid columns",
			builder: NewDB().Insert().Columns("id", "FirstName").Values(u),
			wantErr: errors.New("invalid column id"),
		},
		{
			name:     "no whole columns and multiple values of same type",
			builder:  NewDB().Insert().Columns("Id", "FirstName").Values(u, u1),
			wantSql:  "INSERT INTO `user`(`id`,`first_name`) VALUES(?,?),(?,?);",
			wantArgs: []interface{}{int64(12), "Tom", int64(13), "Jerry"},
		},
		{
			name:    "multiple values of invalid column",
			builder: NewDB().Insert().Values(u, o1),
			wantErr: errors.New("invalid column FirstName"),
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

func ExampleInserter_Build() {
	query, _ := NewDB().Insert().Values(&TestModel{
		Id:  1,
		Age: 18,
	}).Build()
	fmt.Printf("case1\n%s", query.string())

	query, _ = NewDB().Insert().Values(&TestModel{}).Build()
	fmt.Printf("case2\n%s", query.string())

	// Output:
	// case1
	// SQL: INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(?,?,?,?);
	// Args: []interface {}{1, "", 18, (*string)(nil)}
	// case2
	// SQL: INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(?,?,?,?);
	// Args: []interface {}{0, "", 0, (*string)(nil)}
}

func ExampleInserter_Columns() {
	query, _ := NewDB().Insert().Values(&TestModel{
		Id:  1,
		Age: 18,
	}).Columns("Id", "Age").Build()
	fmt.Printf("case1\n%s", query.string())

	query, _ = NewDB().Insert().Values(&TestModel{
		Id:  1,
		Age: 18,
	}, &TestModel{}, &TestModel{FirstName: "Tom"}).Columns("Id", "Age").Build()
	fmt.Printf("case2\n%s", query.string())

	// Output:
	// case1
	// SQL: INSERT INTO `test_model`(`id`,`age`) VALUES(?,?);
	// Args: []interface {}{1, 18}
	// case2
	// SQL: INSERT INTO `test_model`(`id`,`age`) VALUES(?,?),(?,?),(?,?);
	// Args: []interface {}{1, 18, 0, 0, 0, 0}

}

func ExampleInserter_Values() {
	query, _ := NewDB().Insert().Values(&TestModel{
		Id:  1,
		Age: 18,
	}, &TestModel{}).Build()
	fmt.Println(query.string())
	// Output:
	// SQL: INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(?,?,?,?),(?,?,?,?);
	// Args: []interface {}{1, "", 18, (*string)(nil), 0, "", 0, (*string)(nil)}
}
