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

package eorm

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

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
			db, err := Open("sqlite3", "file:test.db?cache=shared&mode=memory",
				DBWithMiddlewares(tc.mdls...))
			if err != nil {
				t.Error(err)
			}
			defer func() {
				_ = db.Close()
			}()
			assert.EqualValues(t, tc.mdls, db.ms)
		})
	}
}

func Test_Middleware_order(t *testing.T) {
	var res []byte
	var mdl1 Middleware = func(next HandleFunc) HandleFunc {
		return func(ctx context.Context, qc *QueryContext) *QueryResult {
			res = append(res, '1')
			return next(ctx, qc)
		}
	}
	var mdl2 Middleware = func(next HandleFunc) HandleFunc {
		return func(ctx context.Context, qc *QueryContext) *QueryResult {
			res = append(res, '2')
			return next(ctx, qc)
		}
	}

	var mdl3 Middleware = func(next HandleFunc) HandleFunc {
		return func(ctx context.Context, qc *QueryContext) *QueryResult {
			res = append(res, '3')
			return next(ctx, qc)
		}
	}
	var last Middleware = func(next HandleFunc) HandleFunc {
		return func(ctx context.Context, qc *QueryContext) *QueryResult {
			return &QueryResult{
				Err: errors.New("mock error"),
			}
		}
	}
	db, err := Open("sqlite3", "file:test.db?cache=shared&mode=memory",
		DBWithMiddlewares(mdl1, mdl2, mdl3, last))
	require.NoError(t, err)

	_, err = NewSelector[TestModel](db).Get(context.Background())
	assert.Equal(t, errors.New("mock error"), err)
	assert.Equal(t, "123", string(res))

}

func TestQueryContext(t *testing.T) {
	testCases := []struct {
		name    string
		wantErr error
		q       Query
		qc      *QueryContext
	}{
		{
			name: "one middleware",
			q: Query{
				SQL:  `SELECT * FROM user_tab WHERE id = ?;`,
				Args: []any{1},
			},
			qc: &QueryContext{
				q: Query{
					SQL:  `SELECT * FROM user_tab WHERE id = ?;`,
					Args: []any{1},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.EqualValues(t, tc.q, tc.qc.GetQuery())
		})
	}
}
