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

package utils

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ScanSuite struct {
	suite.Suite
	mockDB01 *sql.DB
	mock01   sqlmock.Sqlmock
	db02     *sql.DB
}

func (ms *ScanSuite) SetupTest() {
	t := ms.T()
	ms.initMock(t)
}

func (ms *ScanSuite) TearDownTest() {
	_ = ms.mockDB01.Close()
	_ = ms.db02.Close()
}
func (ms *ScanSuite) initMock(t *testing.T) {
	var err error
	query := "CREATE TABLE t1 (\n      id int primary key,\n      `int`  int,\n      `integer` integer,\n      `tinyint` TINYINT,\n      `smallint` smallint,\n      `MEDIUMINT` MEDIUMINT,\n      `BIGINT` BIGINT,\n      `UNSIGNED_BIG_INT` UNSIGNED BIG INT,\n      `INT2` INT2,\n      `INT8` INT8,\n      `VARCHAR` VARCHAR(20),\n  \t\t`CHARACTER` CHARACTER(20),\n  `VARYING_CHARACTER` VARYING_CHARACTER(20),\n  `NCHAR` NCHAR(23),\n  `TEXT` TEXT,\n  `CLOB` CLOB,\n  `REAL` REAL,\n  `DOUBLE` DOUBLE,\n  `DOUBLE_PRECISION` DOUBLE PRECISION,\n  `FLOAT` FLOAT,\n  `DATETIME` DATETIME \n    );"
	ms.mockDB01, ms.mock01, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	db02, err := sql.Open("sqlite3", "file:test01.db?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}
	ms.db02 = db02
	_, err = db02.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
}
func (ms *ScanSuite) TestScan() {
	testcases := []struct {
		name      string
		rows      *sql.Rows
		want      []any
		err       error
		afterFunc func()
	}{
		{
			name: "浮点数",
			rows: func() *sql.Rows {
				cols := []string{"float64"}
				query := "SELECT float64 FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(float64(1.1)))
				rows, err := ms.mockDB01.QueryContext(context.Background(), query)
				require.NoError(ms.T(), err)
				return rows
			}(),
			want: []any{float64(1.1)},
		},
		{
			name: "int64",
			rows: func() *sql.Rows {
				cols := []string{"int64"}
				query := "SELECT int64 FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(int64(1)))
				rows, err := ms.mockDB01.QueryContext(context.Background(), query)
				require.NoError(ms.T(), err)
				return rows
			}(),
			want: []any{int64(1)},
		},
		{
			name: "int32",
			rows: func() *sql.Rows {
				cols := []string{"int32"}
				query := "SELECT int32 FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(int32(1)))
				rows, err := ms.mockDB01.QueryContext(context.Background(), query)
				require.NoError(ms.T(), err)
				return rows
			}(),
			want: []any{int32(1)},
		},
		{
			name: "int16",
			rows: func() *sql.Rows {
				cols := []string{"int16"}
				query := "SELECT int16 FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(int16(1)))
				rows, err := ms.mockDB01.QueryContext(context.Background(), query)
				require.NoError(ms.T(), err)
				return rows
			}(),
			want: []any{int16(1)},
		},
		{
			name: "int8",
			rows: func() *sql.Rows {
				cols := []string{"int8"}
				query := "SELECT int8 FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(int8(1)))
				rows, err := ms.mockDB01.QueryContext(context.Background(), query)
				require.NoError(ms.T(), err)
				return rows
			}(),
			want: []any{int8(1)},
		},
		{
			name: "int",
			rows: func() *sql.Rows {
				cols := []string{"int"}
				query := "SELECT  FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1))
				rows, err := ms.mockDB01.QueryContext(context.Background(), query)
				require.NoError(ms.T(), err)
				return rows
			}(),
			want: []any{1},
		},
		{
			name: "string",
			rows: func() *sql.Rows {
				cols := []string{"string"}
				query := "SELECT string FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("xx"))
				rows, err := ms.mockDB01.QueryContext(context.Background(), query)
				require.NoError(ms.T(), err)
				return rows
			}(),
			want: []any{"string"},
		},
		{
			name: "bool",
			rows: func() *sql.Rows {
				cols := []string{"bool"}
				query := "SELECT bool FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(true))
				rows, err := ms.mockDB01.QueryContext(context.Background(), query)
				require.NoError(ms.T(), err)
				return rows
			}(),
			want: []any{true},
		},
		{
			name: "sqlite3 int类型",
			rows: func() *sql.Rows {
				_, err := ms.db02.Exec("INSERT INTO `t1` (`int`,`integer`,`tinyint`,`smallint`,`MEDIUMINT`,`BIGINT`,`UNSIGNED_BIG_INT`,`INT2`) VALUES (1,1,1,1,1,1,1,1);")
				require.NoError(ms.T(), err)
				query := "SELECT `int`,`integer`,`tinyint`,`smallint`,`MEDIUMINT`,`BIGINT`,`UNSIGNED_BIG_INT`,`INT2`,`INT8` FROM `t1`;"
				rows, err := ms.db02.QueryContext(context.Background(), query)
				require.NoError(ms.T(), err)
				return rows
			}(),
			want: []any{sql.NullInt64{Valid: true, Int64: 1}, sql.NullInt64{Valid: true, Int64: 1}, sql.NullInt64{Valid: true, Int64: 1}, sql.NullInt64{Valid: true, Int64: 1}, sql.NullInt64{Valid: true, Int64: 1}, sql.NullInt64{Valid: true, Int64: 1}, sql.NullInt64{Valid: true, Int64: 1}, sql.NullInt64{Valid: true, Int64: 1}, sql.NullInt64{Valid: false, Int64: 0}},
			afterFunc: func() {
				_, err := ms.db02.Exec("truncate table `t1`")
				require.NoError(ms.T(), err)
			},
		},
		{
			name: "sqlite3 string类型",
			rows: func() *sql.Rows {
				_, err := ms.db02.Exec("INSERT INTO `t1` (`VARCHAR`,`CHARACTER`,`VARYING_CHARACTER`,`NCHAR`,`TEXT`) VALUES ('zwl','zwl','zwl','zwl','zwl');")
				require.NoError(ms.T(), err)
				query := "SELECT `VARCHAR`,`CHARACTER`,`VARYING_CHARACTER`,`NCHAR`,`TEXT`,`CLOB` FROM `t1`;"
				rows, err := ms.db02.QueryContext(context.Background(), query)
				require.NoError(ms.T(), err)
				return rows
			}(),
			want: []any{sql.NullString{Valid: true, String: "zwl"}, sql.NullString{Valid: true, String: "zwl"}, sql.NullString{Valid: true, String: "zwl"}, sql.NullString{Valid: true, String: "zwl"}, sql.NullString{Valid: true, String: "zwl"}, sql.NullString{Valid: false, String: ""}},
			afterFunc: func() {
				_, err := ms.db02.Exec("truncate table `t1`")
				require.NoError(ms.T(), err)
			},
		},
		{
			name: "sqlite3 浮点类型",
			rows: func() *sql.Rows {
				_, err := ms.db02.Exec("INSERT INTO `t1` (`REAL`,`DOUBLE`,`DOUBLE_PRECISION`) VALUES (1.0,1.0,1.0);")
				require.NoError(ms.T(), err)
				query := "SELECT `REAL`,`DOUBLE`,`DOUBLE_PRECISION`,`FLOAT` FROM `t1`;"
				rows, err := ms.db02.QueryContext(context.Background(), query)
				require.NoError(ms.T(), err)
				return rows
			}(),
			want: []any{sql.NullFloat64{Valid: true, Float64: 1.0}, sql.NullFloat64{Valid: true, Float64: 1.0}, sql.NullFloat64{Valid: true, Float64: 1.0}, sql.NullFloat64{Valid: false, Float64: 0}},
			afterFunc: func() {
				_, err := ms.db02.Exec("truncate table `t1`")
				require.NoError(ms.T(), err)
			},
		},
		{
			name: "sqlite3时间类型",
			rows: func() *sql.Rows {
				_, err := ms.db02.Exec("INSERT INTO `t1` (`DATETIME`) VALUES ('2022-01-01 12:00:00');")
				require.NoError(ms.T(), err)
				query := "SELECT `DATETIME` FROM `t1`;"
				rows, err := ms.db02.QueryContext(context.Background(), query)
				require.NoError(ms.T(), err)
				return rows
			}(),
			want: []any{sql.NullTime{Valid: true, Time: func() time.Time {
				t, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 12:00:00", time.Local)
				require.NoError(ms.T(), err)
				return t

			}()}},
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			rows := tc.rows
			require.True(t, rows.Next())
			got, err := Scan(rows)
			require.Equal(t, tc.err, err)
			if err == nil {
				return
			}
			require.Equal(t, tc.want, got)
			tc.afterFunc()
		})
	}
}

func TestMerger(t *testing.T) {
	suite.Run(t, &ScanSuite{})
}
