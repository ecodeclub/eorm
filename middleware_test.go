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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Middleware(t *testing.T) {
	testCases := []struct {
		name    string
		wantErr error
		mdls    []Middleware
	}{
		{
			name: "one middleware",
			mdls: func() []Middleware {
				var mdl Middleware = func(next HandleFunc) HandleFunc {
					return func(ctx context.Context, queryContext *QueryContext) *QueryResult {
						return &QueryResult{}
					}
				}
				return []Middleware{mdl}
			}(),
		},
		{
			name: "many middleware",
			mdls: func() []Middleware {
				mdl1 := func(next HandleFunc) HandleFunc {
					return func(ctx context.Context, queryContext *QueryContext) *QueryResult {
						return &QueryResult{Result: "mdl1"}
					}
				}
				mdl2 := func(next HandleFunc) HandleFunc {
					return func(ctx context.Context, queryContext *QueryContext) *QueryResult {
						return &QueryResult{Result: "mdl2"}
					}
				}
				return []Middleware{mdl1, mdl2}
			}(),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			orm, err := Open("sqlite3", "file:test.db?cache=shared&mode=memory",
				DBWithMiddleware(tc.mdls...))
			if err != nil {
				t.Error(err)
			}
			defer func() {
				_ = orm.Close()
			}()
			assert.EqualValues(t, tc.mdls, orm.ms)
		})
	}
}
