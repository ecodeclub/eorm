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

package querylog

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/gotomicro/eorm"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestMiddlewareBuilder_Build(t *testing.T) {
	testCases := []struct {
		name    string
		builder *MiddlewareBuilder
		wantVal any
	}{
		{
			name:    "not args",
			builder: NewBuilder(),
		},
		{
			name:    "output args",
			builder: NewBuilder().OutputArgs(true),
		},
		{
			name: "log func",
			builder: func() *MiddlewareBuilder {
				builder := NewBuilder()
				builder.LogFunc(func(sql string, args ...any) {
					fmt.Println(sql)
				})
				return builder
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := eorm.Open("sqlite3",
				"file:test.db?cache=shared&mode=memory", eorm.DBWithMiddlewares(
					tc.builder.Build()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = db.Close()
			}()
			_, err = eorm.NewSelector[TestModel](db).Get(context.Background())
			assert.NotNil(t, err)
		})
	}

}

type TestModel struct {
	Id        int64 `eorm:"auto_increment,primary_key"`
	FirstName string
	Age       int8
	LastName  *sql.NullString
}
