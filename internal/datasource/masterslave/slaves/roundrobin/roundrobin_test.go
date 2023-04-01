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

package roundrobin

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"

	"github.com/ecodeclub/eorm/internal/errs"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ExampleSlaves_Close() {
	db, _ := sql.Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	sl, _ := NewSlaves(db)
	if err := sl.Close(); err == nil {
		fmt.Println("close")
	}

	// Output:
	// close
}

func TestSlaves_Next(t *testing.T) {
	db1 := &sql.DB{}
	db2 := &sql.DB{}
	db3 := &sql.DB{}
	testCases := []struct {
		name   string
		slaves func() slaves.Slaves
		ctx    context.Context

		wantErr error
		wantDB  *sql.DB
	}{
		{
			name: "ctx error",
			ctx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				// 直接 cancel 确保 ctx.Error 返回 error
				cancel()
				return ctx
			}(),
			slaves: func() slaves.Slaves {
				res, err := NewSlaves(db1, db2, db3)
				require.NoError(t, err)
				return res
			},
			wantErr: context.Canceled,
		},
		{
			name: "no slaves",
			ctx:  context.Background(),
			slaves: func() slaves.Slaves {
				res, err := NewSlaves()
				require.NoError(t, err)
				return res
			},
			wantErr: errs.ErrSlaveNotFound,
		},
		{
			name: "index 0",
			ctx:  context.Background(),
			slaves: func() slaves.Slaves {
				res, err := NewSlaves(db1, db2, db3)
				require.NoError(t, err)
				return res
			},
			wantDB: db1,
		},
		{
			name: "index last",
			ctx:  context.Background(),
			slaves: func() slaves.Slaves {
				sl, err := NewSlaves(db1, db2, db3)
				require.NoError(t, err)
				sl.cnt = 1
				return sl
			},
			wantDB: db3,
		},
		{
			name: "jump to first",
			ctx:  context.Background(),
			slaves: func() slaves.Slaves {
				sl, err := NewSlaves(db1, db2, db3)
				require.NoError(t, err)
				sl.cnt = 2
				return sl
			},
			wantDB: db1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := tc.slaves().Next(tc.ctx)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantDB, db.DB)
		})
	}
}
