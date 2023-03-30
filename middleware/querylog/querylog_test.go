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

package querylog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ecodeclub/eorm"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestMiddlewareBuilder_Build(t *testing.T) {
	testCases := []struct {
		name    string
		mdls    []eorm.Middleware
		builder *testMiddlewareBuilder
		wantVal string
		wantErr error
	}{
		{
			name: "default",
			builder: &testMiddlewareBuilder{
				MiddlewareBuilder: NewBuilder(),
				printVal:          strings.Builder{},
			},
			mdls: []eorm.Middleware{},
		},
		{
			name: "output args",
			builder: func() *testMiddlewareBuilder {
				b := &testMiddlewareBuilder{
					MiddlewareBuilder: NewBuilder(),
					printVal:          strings.Builder{},
				}
				logfunc := func(sql string, args ...any) {
					fmt.Println(sql, args)
					b.printVal.WriteString(sql)
				}
				b.LogFunc(logfunc)
				return b
			}(),
			mdls:    []eorm.Middleware{},
			wantVal: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` LIMIT ?;",
		},
		{
			name: "not args",
			builder: &testMiddlewareBuilder{
				printVal: strings.Builder{},
				MiddlewareBuilder: NewBuilder().LogFunc(func(sql string, args ...any) {
					fmt.Println(sql)
				}),
			},
			mdls:    []eorm.Middleware{},
			wantVal: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` LIMIT ?;",
		},
		{
			name: "interrupt err",
			builder: &testMiddlewareBuilder{
				printVal: strings.Builder{},
				MiddlewareBuilder: NewBuilder().LogFunc(func(sql string, args ...any) {
					fmt.Println(sql)
				}),
			},
			mdls: func() []eorm.Middleware {
				var interrupt eorm.Middleware = func(next eorm.HandleFunc) eorm.HandleFunc {
					return func(ctx context.Context, qc *eorm.QueryContext) *eorm.QueryResult {
						return &eorm.QueryResult{
							Err: errors.New("interrupt execution"),
						}
					}
				}
				return []eorm.Middleware{interrupt}
			}(),
			wantErr: errors.New("interrupt execution"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mdls := tc.mdls
			mdls = append(mdls, tc.builder.Build())
			orm, err := eorm.Open("sqlite3",
				"file:test.db?cache=shared&mode=memory",
				eorm.DBWithMiddlewares(mdls...))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = orm.Close()
			}()
			_, err = eorm.NewSelector[TestModel](orm).Get(context.Background())
			if err.Error() == "no such table: test_model" {
				return
			}
			if err != nil {
				assert.Equal(t, tc.wantErr, err)
				return
			}
			assert.Equal(t, tc.wantVal, tc.builder.printVal.String())
		})
	}

}

type testMiddlewareBuilder struct {
	*MiddlewareBuilder
	printVal strings.Builder
}

type TestModel struct {
	Id        int64 `eorm:"primary_key"`
	FirstName string
	Age       int8
	LastName  *sql.NullString
}
