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

package dialect

import (
	"testing"

	"github.com/gotomicro/eorm/internal/errs"
	"github.com/stretchr/testify/assert"
)

func TestOf(t *testing.T) {
	testCases := []struct {
		name        string
		driver      string
		wantErr     error
		wantDialect Dialect
	}{
		{
			name:        "mysql",
			driver:      "mysql",
			wantDialect: MySQL,
		},
		{
			name:        "sqlite3",
			driver:      "sqlite3",
			wantDialect: SQLite,
		},
		{
			name:        "mssql",
			driver:      "mssql",
			wantDialect: SQLServer,
		},
		{
			name:        "sqlserver",
			driver:      "sqlserver",
			wantDialect: SQLServer,
		},
		{
			name:    "unsupported",
			driver:  "abc",
			wantErr: errs.NewUnsupportedDriverError("abc"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dialect, err := Of(tc.driver)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantDialect, dialect)
		})
	}
}
