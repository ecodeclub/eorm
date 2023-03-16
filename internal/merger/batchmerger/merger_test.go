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

package batchmerger

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type MergerSuite struct {
	suite.Suite
	mockDB01 *sql.DB
	mock01   sqlmock.Sqlmock
	mockDB02 *sql.DB
	mock02   sqlmock.Sqlmock
	mockDB03 *sql.DB
	mock03   sqlmock.Sqlmock
	mockDB04 *sql.DB
	mock04   sqlmock.Sqlmock
}

func (ms *MergerSuite) SetupTest() {
	t := ms.T()
	ms.initMock(t)
}

func (ms *MergerSuite) TearDownTest() {
	_ = ms.mockDB01.Close()
	_ = ms.mockDB02.Close()
	_ = ms.mockDB03.Close()
	_ = ms.mockDB04.Close()
}

func (ms *MergerSuite) initMock(t *testing.T) {
	var err error
	ms.mockDB01, ms.mock01, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	ms.mockDB02, ms.mock02, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	ms.mockDB03, ms.mock03, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	ms.mockDB04, ms.mock04, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
}

func (ms *MergerSuite) TestMerger_NextAndScan() {

	testCases := []struct {
		name    string
		sqlRows func() []*sql.Rows
		wantVal []string
		wantErr error
		scanErr error
	}{
		{
			name: "multi rows",
			sqlRows: func() []*sql.Rows {
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("1"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("2"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("3").AddRow("4"))
				res := make([]*sql.Rows, 0, 3)
				row01, _ := ms.mockDB01.QueryContext(context.Background(), "SELECT * FROM `t1`;")
				res = append(res, row01)
				row02, _ := ms.mockDB02.QueryContext(context.Background(), "SELECT * FROM `t1`;")
				res = append(res, row02)
				row03, _ := ms.mockDB03.QueryContext(context.Background(), "SELECT * FROM `t1`;")
				res = append(res, row03)
				return res
			},
			wantVal: []string{"1", "2", "3", "4"},
		},
		{
			name: "empty rows",
			sqlRows: func() []*sql.Rows {
				return []*sql.Rows{}
			},
			wantErr: errs.ErrMergerEmptyRows,
		},
		{
			name: "nil sqlrows",
			sqlRows: func() []*sql.Rows {
				return []*sql.Rows{nil}
			},
			wantErr: errs.ErrMergerRowsIsNull,
		},
		{
			// sqlrows列表中有一个没有查询到值
			name: "sqlrows has empty rows",
			sqlRows: func() []*sql.Rows {
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("1"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("2"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("3").AddRow("4"))
				ms.mock04.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}))
				res := make([]*sql.Rows, 0, 4)
				row01, _ := ms.mockDB01.QueryContext(context.Background(), "SELECT * FROM `t1`;")
				res = append(res, row01)
				row02, _ := ms.mockDB02.QueryContext(context.Background(), "SELECT * FROM `t1`;")
				res = append(res, row02)
				row04, _ := ms.mockDB04.QueryContext(context.Background(), "SELECT * FROM `t1`;")
				res = append(res, row04)
				row03, _ := ms.mockDB03.QueryContext(context.Background(), "SELECT * FROM `t1`;")
				res = append(res, row03)
				return res
			},
			wantVal: []string{"1", "2", "3", "4"},
		},
		{
			// 几个sql.rows里都没有数据
			name: "mutil sqlrows but has no rows",
			sqlRows: func() []*sql.Rows {
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}))
				res := make([]*sql.Rows, 0, 3)
				row01, _ := ms.mockDB01.QueryContext(context.Background(), "SELECT * FROM `t1`;")
				res = append(res, row01)
				row02, _ := ms.mockDB02.QueryContext(context.Background(), "SELECT * FROM `t1`;")
				res = append(res, row02)
				row03, _ := ms.mockDB03.QueryContext(context.Background(), "SELECT * FROM `t1`;")
				res = append(res, row03)
				return res
			},
			wantVal: []string{},
		},
	}
	for _, tc := range testCases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger := Merger{}
			rows, err := merger.Merge(context.Background(), tc.sqlRows())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			res := make([]string, 0, 4)
			for rows.Next() {
				var id string
				err = rows.Scan(&id)
				assert.Equal(t, tc.scanErr, err)
				if err != nil {
					return
				}
				res = append(res, id)
			}
			assert.Equal(t, tc.wantVal, res)
		})
	}

}
func (ms *MergerSuite) TestClose() {
	ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("1"))
	ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("2"))
	ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("3").AddRow("4"))
	ms.T().Run("close", func(t *testing.T) {
		merger := Merger{}
		res := make([]*sql.Rows, 0, 3)
		row01, _ := ms.mockDB01.QueryContext(context.Background(), "SELECT * FROM `t1`;")
		res = append(res, row01)
		row02, _ := ms.mockDB02.QueryContext(context.Background(), "SELECT * FROM `t1`;")
		res = append(res, row02)
		row03, _ := ms.mockDB03.QueryContext(context.Background(), "SELECT * FROM `t1`;")
		res = append(res, row03)
		rows, err := merger.Merge(context.Background(), res)
		require.NoError(t, err)
		if err != nil {
			return
		}
		err = rows.Close()
		require.NoError(t, err)
		err = rows.Close()
		require.NoError(t, err)
		err = rows.Close()
		require.NoError(t, err)
	})
}
func (ms *MergerSuite) TestColumns() {
	ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("1"))
	ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("2"))
	ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("3").AddRow("4"))
	testCases := []struct {
		name     string
		wantCols []string
		sqlRows  func() []*sql.Rows
	}{
		{
			name:     "Columns",
			wantCols: []string{"id"},
			sqlRows: func() []*sql.Rows {
				res := make([]*sql.Rows, 0, 3)
				row01, _ := ms.mockDB01.QueryContext(context.Background(), "SELECT * FROM `t1`;")
				res = append(res, row01)
				row02, _ := ms.mockDB02.QueryContext(context.Background(), "SELECT * FROM `t1`;")
				res = append(res, row02)
				row03, _ := ms.mockDB03.QueryContext(context.Background(), "SELECT * FROM `t1`;")
				res = append(res, row03)
				return res
			},
		},
	}
	for _, tc := range testCases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger := Merger{}
			rows, err := merger.Merge(context.Background(), tc.sqlRows())
			require.NoError(ms.T(), err)
			if err != nil {
				return
			}
			cols, err := rows.Columns()
			require.NoError(ms.T(), err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantCols, cols)
		})

	}
}

func TestMerger(t *testing.T) {
	suite.Run(t, &MergerSuite{})
}
