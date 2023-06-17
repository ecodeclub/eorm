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

package sortmerger

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	"github.com/ecodeclub/eorm/internal/merger/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/multierr"
)

var mockErr error = errors.New("mock error")

func TestMerger_NewMerge(t *testing.T) {
	testcases := []struct {
		name         string
		sortCols     SortColumns
		distinctCols []merger.ColumnInfo
		wantErr      error
	}{
		{
			name: "Valid case",
			sortCols: SortColumns{
				columns: []SortColumn{
					NewSortColumn("column1", utils.ASC),
					NewSortColumn("column2", utils.DESC),
				},
				colMap: map[string]int{
					"column1": 0,
					"column2": 1,
				},
			},
			distinctCols: []merger.ColumnInfo{
				merger.NewColumnInfo(0, "column1"),
				merger.NewColumnInfo(1, "column2"),
			},
			wantErr: nil,
		},
		{
			name: "重复的去重列",
			sortCols: SortColumns{
				columns: []SortColumn{
					NewSortColumn("column1", utils.ASC),
					NewSortColumn("column2", utils.DESC),
				},
				colMap: map[string]int{
					"column1": 0,
					"column2": 1,
				},
			},
			distinctCols: []merger.ColumnInfo{
				merger.NewColumnInfo(0, "column1"),
				merger.NewColumnInfo(1, "column1"),
			},
			wantErr: errs.ErrDistinctColsRepeated,
		},
		{
			name: "排序列有额外的列",
			sortCols: SortColumns{
				columns: []SortColumn{
					NewSortColumn("column1", utils.ASC),
					NewSortColumn("column2", utils.DESC),
				},
				colMap: map[string]int{
					"column1": 0,
					"column2": 1,
				},
			},
			distinctCols: []merger.ColumnInfo{
				merger.NewColumnInfo(0, "column1"),
			},
			wantErr: errs.ErrSortColListNotContainDistinctCol,
		},
		{
			name: "空的排序列和空的去重列",
			sortCols: SortColumns{
				columns: []SortColumn{},
				colMap:  map[string]int{},
			},
			distinctCols: []merger.ColumnInfo{},
			wantErr:      nil,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewDistinctMerger(tc.distinctCols, tc.sortCols)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			require.NotNil(t, m)
		})
	}
}

type DistinctMergerSuite struct {
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

func (ms *DistinctMergerSuite) SetupTest() {
	t := ms.T()
	ms.initMock(t)
}

func (ms *DistinctMergerSuite) TearDownTest() {
	_ = ms.mockDB01.Close()
	_ = ms.mockDB02.Close()
	_ = ms.mockDB03.Close()
	_ = ms.mockDB04.Close()
}

func (ms *DistinctMergerSuite) initMock(t *testing.T) {
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

func (ms *DistinctMergerSuite) TestOrderByMerger_Merge() {
	testcases := []struct {
		name    string
		merger  func() (*DistinctMerger, error)
		ctx     func() (context.Context, context.CancelFunc)
		wantErr error
		sqlRows func() []*sql.Rows
	}{
		{
			name: "sqlRows字段不同",
			merger: func() (*DistinctMerger, error) {
				sortcols, err := newSortColumns(NewSortColumn("id", utils.ASC))
				require.NoError(ms.T(), err)
				return NewDistinctMerger([]merger.ColumnInfo{merger.NewColumnInfo(0, "id"), merger.NewColumnInfo(2, "name"), merger.NewColumnInfo(3, "address")}, sortcols)
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			sqlRows: func() []*sql.Rows {
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "address"}).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantErr: errs.ErrDistinctColsNotInCols,
		},
		{
			name: "sqlRows字段不同_少一个字段",
			merger: func() (*DistinctMerger, error) {
				sortcols, err := newSortColumns(NewSortColumn("id", utils.ASC))
				require.NoError(ms.T(), err)
				return NewDistinctMerger([]merger.ColumnInfo{merger.NewColumnInfo(0, "id"), merger.NewColumnInfo(2, "name"), merger.NewColumnInfo(3, "address")}, sortcols)

			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			sqlRows: func() []*sql.Rows {
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "address"}).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(3, "alex").AddRow(4, "x"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantErr: errs.ErrDistinctColsNotInCols,
		},
		{
			name: "sqlRows列表为空",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			merger: func() (*DistinctMerger, error) {
				sortcols, err := newSortColumns(NewSortColumn("id", utils.ASC))
				require.NoError(ms.T(), err)
				return NewDistinctMerger([]merger.ColumnInfo{merger.NewColumnInfo(0, "id"), merger.NewColumnInfo(2, "name"), merger.NewColumnInfo(3, "address")}, sortcols)

			},
			sqlRows: func() []*sql.Rows {
				return []*sql.Rows{}
			},
			wantErr: errs.ErrMergerEmptyRows,
		},
		{
			name: "sqlRows列表有nil",
			merger: func() (*DistinctMerger, error) {
				sortcols, err := newSortColumns(NewSortColumn("id", utils.ASC))
				require.NoError(ms.T(), err)
				return NewDistinctMerger([]merger.ColumnInfo{merger.NewColumnInfo(0, "id"), merger.NewColumnInfo(2, "name"), merger.NewColumnInfo(3, "address")}, sortcols)
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			sqlRows: func() []*sql.Rows {
				return []*sql.Rows{nil}
			},
			wantErr: errs.ErrMergerRowsIsNull,
		},
		{
			name: "数据库中的列不包含distinct的列",
			merger: func() (*DistinctMerger, error) {
				sortcols, err := newSortColumns(NewSortColumn("id", utils.ASC))
				require.NoError(ms.T(), err)
				return NewDistinctMerger([]merger.ColumnInfo{merger.NewColumnInfo(0, "id"), merger.NewColumnInfo(2, "name"), merger.NewColumnInfo(3, "address")}, sortcols)
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			sqlRows: func() []*sql.Rows {
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "address"}).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantErr: errs.ErrDistinctColsNotInCols,
		},
		{
			name: "数据库中的列顺序和distinct的列顺序不一致",
			merger: func() (*DistinctMerger, error) {
				sortcols, err := newSortColumns(NewSortColumn("id", utils.ASC))
				require.NoError(ms.T(), err)
				return NewDistinctMerger([]merger.ColumnInfo{merger.NewColumnInfo(0, "id"), merger.NewColumnInfo(1, "name"), merger.NewColumnInfo(2, "address")}, sortcols)
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			sqlRows: func() []*sql.Rows {
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id", "email", "name"}).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantErr: errs.ErrDistinctColsNotInCols,
		},
	}

	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger, err := tc.merger()
			require.NoError(ms.T(), err)
			ctx, cancel := tc.ctx()
			rows, err := merger.Merge(ctx, tc.sqlRows())
			cancel()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			require.NotNil(t, rows)
		})
	}
}

func (ms *DistinctMergerSuite) TestOrderByRows_NextAndScan() {
	testcases := []struct {
		name            string
		sqlRows         func() []*sql.Rows
		wantVal         []TestModel
		sortColumns     SortColumns
		distinctColumns []merger.ColumnInfo
		wantErr         error
	}{
		{
			name: "所有的列全部相同",
			sqlRows: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{
					Id:      1,
					Name:    "abex",
					Address: "cn",
				},
			},
			sortColumns: func() SortColumns {
				cols, err := newSortColumns(NewSortColumn("id", utils.DESC))
				require.NoError(ms.T(), err)
				return cols
			}(),
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0, Name: "id",
				},
				{
					Index: 1, Name: "name",
				},
				{
					Index: 2, Name: "address",
				},
			},
		},
		{
			name: "部分列相同",
			sqlRows: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "abex", "kn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "alex", "cn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			sortColumns: func() SortColumns {
				cols, err := newSortColumns(NewSortColumn("id", utils.DESC))
				require.NoError(ms.T(), err)
				return cols
			}(),
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0, Name: "id",
				},
				{
					Index: 1, Name: "name",
				},
				{
					Index: 2, Name: "address",
				},
			},
			wantVal: []TestModel{
				{2, "alex", "cn"},
				{1, "abex", "cn"},
				{1, "abex", "kn"},
				{1, "alex", "cn"},
			},
		},
		{
			name: "有多个顺序列相同的情况",
			sortColumns: func() SortColumns {
				cols, err := newSortColumns(NewSortColumn("id", utils.ASC))
				require.NoError(ms.T(), err)
				return cols
			}(),
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0,
					Name:  "id",
				},
				{
					Index: 1,
					Name:  "name",
				},
				{
					Index: 2,
					Name:  "address",
				},
			},
			sqlRows: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "abex", "kn").AddRow(2, "alex", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "alex", "cn").AddRow(2, "alex", "kn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn").AddRow(2, "alex", "kn").AddRow(3, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{1, "abex", "cn"},
				{1, "abex", "kn"},
				{1, "alex", "cn"},
				{2, "alex", "cn"},
				{2, "alex", "kn"},
				{3, "alex", "cn"},
			},
		},
		{
			name: "多个排序列，Order by id anme,distinct id name address",
			sortColumns: func() SortColumns {
				cols, err := newSortColumns(NewSortColumn("id", utils.ASC), NewSortColumn("name", utils.DESC))
				require.NoError(ms.T(), err)
				return cols
			}(),
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0,
					Name:  "id",
				},
				{
					Index: 1,
					Name:  "name",
				},
				{
					Index: 2,
					Name:  "address",
				},
			},
			sqlRows: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "abex", "kn").AddRow(2, "alex", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "alex", "cn").AddRow(1, "abex", "cn").AddRow(2, "alex", "kn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn").AddRow(2, "alex", "kn").AddRow(3, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{1, "alex", "cn"},
				{1, "abex", "cn"},
				{1, "abex", "kn"},
				{2, "alex", "cn"},
				{2, "alex", "kn"},
				{3, "alex", "cn"},
			},
		},
		{
			name: "多个排序列，Order by id address,distinct id name address",
			sortColumns: func() SortColumns {
				cols, err := newSortColumns(NewSortColumn("id", utils.ASC), NewSortColumn("address", utils.ASC))
				require.NoError(ms.T(), err)
				return cols
			}(),
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0,
					Name:  "id",
				},
				{
					Index: 1,
					Name:  "name",
				},
				{
					Index: 2,
					Name:  "address",
				},
			},
			sqlRows: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "abex", "kn").AddRow(2, "alex", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "alex", "cn").AddRow(1, "abex", "cn").AddRow(2, "alex", "kn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn").AddRow(2, "alex", "kn").AddRow(3, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{1, "abex", "cn"},
				{1, "alex", "cn"},
				{1, "abex", "kn"},
				{2, "alex", "cn"},
				{2, "alex", "kn"},
				{3, "alex", "cn"},
			},
		},
		{
			name: "Order by name, distinct id name address",
			sortColumns: func() SortColumns {
				cols, err := newSortColumns(NewSortColumn("name", utils.ASC))
				require.NoError(ms.T(), err)
				return cols
			}(),
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0,
					Name:  "id",
				},
				{
					Index: 1,
					Name:  "name",
				},
				{
					Index: 2,
					Name:  "address",
				},
			},
			sqlRows: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "abex", "kn").AddRow(2, "alex", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "alex", "cn").AddRow(2, "alex", "kn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn").AddRow(2, "alex", "kn").AddRow(3, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{1, "abex", "cn"},
				{1, "abex", "kn"},
				{1, "alex", "cn"},
				{2, "alex", "cn"},
				{2, "alex", "kn"},
				{3, "alex", "cn"},
			},
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			m, err := NewDistinctMerger(tc.distinctColumns, tc.sortColumns)
			require.NoError(t, err)
			rows, err := m.Merge(context.Background(), tc.sqlRows())
			require.NoError(t, err)
			ans := make([]TestModel, 0, len(tc.wantVal))
			for rows.Next() {
				t := TestModel{}
				err = rows.Scan(&t.Id, &t.Name, &t.Address)
				require.NoError(ms.T(), err)
				ans = append(ans, t)
			}
			assert.Equal(t, tc.wantVal, ans)
		})
	}
}

func (ms *DistinctMergerSuite) TestOrderByRows_NotHaveOrderBy() {
	testcases := []struct {
		name            string
		wantVal         []TestModel
		distinctColumns []merger.ColumnInfo
		wantErr         error
		sqlRows         func() []*sql.Rows
	}{
		{
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0,
					Name:  "id",
				},
				{
					Index: 1,
					Name:  "name",
				},
				{
					Index: 2,
					Name:  "address",
				},
			},
			name: "去重未含orderby",
			sqlRows: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "abex", "k"+
					"n"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "alex", "cn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "alex", "cn").AddRow(2, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{1, "abex", "cn"},
				{1, "abex", "kn"},
				{1, "alex", "cn"},
				{2, "alex", "cn"},
			},
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			m, err := NewDistinctMerger(tc.distinctColumns)
			require.NoError(t, err)
			rows, err := m.Merge(context.Background(), tc.sqlRows())
			require.NoError(t, err)
			ans := make([]TestModel, 0, len(tc.wantVal))
			for rows.Next() {
				t := TestModel{}
				err = rows.Scan(&t.Id, &t.Name, &t.Address)
				require.NoError(ms.T(), err)
				ans = append(ans, t)
			}
			assert.Equal(t, tc.wantVal, ans)
		})
	}
}

func (ms *DistinctMergerSuite) TestOrderByRows_NextAndErr() {
	testcases := []struct {
		name            string
		rowsList        func() []*sql.Rows
		wantErr         error
		sortColumns     SortColumns
		distinctColumns []merger.ColumnInfo
	}{
		{
			name: "sqlRows列表中有一个返回error",
			rowsList: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "abex", "kn").RowError(1, mockErr))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "alex", "cn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			sortColumns: func() SortColumns {
				cols, err := newSortColumns(NewSortColumn("id", utils.DESC))
				require.NoError(ms.T(), err)
				return cols
			}(),
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0, Name: "id",
				},
				{
					Index: 1, Name: "name",
				},
				{
					Index: 2, Name: "address",
				},
			},
			wantErr: mockErr,
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger, err := NewDistinctMerger(tc.distinctColumns, tc.sortColumns)
			require.NoError(t, err)
			rows, err := merger.Merge(context.Background(), tc.rowsList())
			require.NoError(t, err)
			for rows.Next() {
			}
			assert.Equal(t, tc.wantErr, rows.Err())
		})
	}
}

func (ms *DistinctMergerSuite) TestOrderByRows_Columns() {
	cols := []string{"id", "name", "address"}
	query := "SELECT * FROM `t1`"
	ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "abex", "kn"))
	ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(1, "alex", "cn"))
	ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn"))
	sortCols, err := newSortColumns(NewSortColumn("id", utils.DESC))
	require.NoError(ms.T(), err)
	merger, err := NewDistinctMerger([]merger.ColumnInfo{
		{
			Index: 0, Name: "id",
		},
		{
			Index: 1, Name: "name",
		},
		{
			Index: 2, Name: "address",
		},
	}, sortCols)
	require.NoError(ms.T(), err)
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]*sql.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), query)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}
	rows, err := merger.Merge(context.Background(), rowsList)
	require.NoError(ms.T(), err)
	ms.T().Run("Next没有迭代完", func(t *testing.T) {
		for rows.Next() {
			columns, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, cols, columns)
		}
		require.NoError(t, rows.Err())
	})
	ms.T().Run("Next迭代完", func(t *testing.T) {
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		_, err := rows.Columns()
		assert.Equal(t, errs.ErrMergerRowsClosed, err)
	})
}

func (ms *DistinctMergerSuite) TestOrderByRows_Close() {
	cols := []string{"id"}
	query := "SELECT * FROM `t1`"
	ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("1"))
	ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("2").AddRow("5").AddRow("6").CloseError(newCloseMockErr("db02")))
	ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("5").AddRow("7").CloseError(newCloseMockErr("db03")))
	sortCols, err := newSortColumns(NewSortColumn("id", utils.DESC))
	require.NoError(ms.T(), err)
	merger, err := NewDistinctMerger([]merger.ColumnInfo{
		{
			Index: 0, Name: "id",
		},
	}, sortCols)
	require.NoError(ms.T(), err)
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]*sql.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), query)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}
	rows, err := merger.Merge(context.Background(), rowsList)
	require.NoError(ms.T(), err)
	// 判断当前是可以正常读取的
	require.True(ms.T(), rows.Next())
	var id int
	err = rows.Scan(&id)
	require.NoError(ms.T(), err)
	err = rows.Close()
	ms.T().Run("close返回multierror", func(t *testing.T) {
		assert.Equal(ms.T(), multierr.Combine(newCloseMockErr("db02"), newCloseMockErr("db03")), err)
	})
	ms.T().Run("close之后Next返回false", func(t *testing.T) {
		for i := 0; i < len(rowsList); i++ {
			require.False(ms.T(), rowsList[i].Next())
		}
		require.False(ms.T(), rows.Next())
	})
	ms.T().Run("close之后Scan返回迭代过程中的错误", func(t *testing.T) {
		var id int
		err := rows.Scan(&id)
		assert.Equal(t, errs.ErrMergerRowsClosed, err)
	})
	ms.T().Run("close之后调用Columns方法返回错误", func(t *testing.T) {
		_, err := rows.Columns()
		require.Error(t, err)
	})
	ms.T().Run("close多次是等效的", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			err = rows.Close()
			require.NoError(t, err)
		}
	})
}

func (ms *DistinctMergerSuite) TestOrderByRows_Scan() {
	ms.T().Run("未调用Next，直接Scan，返回错", func(t *testing.T) {
		cols := []string{"id", "name", "address"}
		query := "SELECT * FROM `t1`"
		ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
		r, err := ms.mockDB01.QueryContext(context.Background(), query)
		require.NoError(t, err)
		rowsList := []*sql.Rows{r}
		sortCols, err := newSortColumns(NewSortColumn("id", utils.DESC))
		require.NoError(t, err)
		merger, err := NewDistinctMerger([]merger.ColumnInfo{
			{
				Index: 0, Name: "id",
			},
			{
				Index: 1, Name: "name",
			},
			{
				Index: 2, Name: "address",
			},
		}, sortCols)
		require.NoError(t, err)
		rows, err := merger.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		model := TestModel{}
		err = rows.Scan(&model.Id, &model.Name, &model.Address)
		assert.Equal(t, errs.ErrMergerScanNotNext, err)
	})
	ms.T().Run("迭代过程中发现错误,调用Scan，返回迭代中发现的错误", func(t *testing.T) {
		cols := []string{"id", "name", "address"}
		query := "SELECT * FROM `t1`"
		ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn").AddRow(6, "bruce", "cn").RowError(2, mockErr))
		r, err := ms.mockDB01.QueryContext(context.Background(), query)
		require.NoError(t, err)
		rowsList := []*sql.Rows{r}
		sortCols, err := newSortColumns(NewSortColumn("id", utils.DESC))
		require.NoError(t, err)
		merger, err := NewDistinctMerger([]merger.ColumnInfo{
			{
				Index: 0, Name: "id",
			},
			{
				Index: 1, Name: "name",
			},
			{
				Index: 2, Name: "address",
			},
		}, sortCols)
		require.NoError(t, err)
		rows, err := merger.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		for rows.Next() {
		}
		var model TestModel
		err = rows.Scan(&model.Id, &model.Name, &model.Address)
		assert.Equal(t, mockErr, err)
	})
}

func TestOrderbyMerger(t *testing.T) {
	suite.Run(t, &DistinctMergerSuite{})
	suite.Run(t, &NullableOrderByMergerSuite{})
}

type NullableOrderByMergerSuite struct {
	suite.Suite
	db01 *sql.DB
	db02 *sql.DB
	db03 *sql.DB
}

func (ms *NullableOrderByMergerSuite) SetupSuite() {
	t := ms.T()
	query := "CREATE TABLE t1 (\n      id int primary key,\n      `age`  int,\n    \t`name` varchar(20)\n  );\n"
	db01, err := sql.Open("sqlite3", "file:test01.db?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}
	ms.db01 = db01
	_, err = db01.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	db02, err := sql.Open("sqlite3", "file:test02.db?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}
	ms.db02 = db02
	_, err = db02.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	db03, err := sql.Open("sqlite3", "file:test03.db?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}
	ms.db03 = db03
	_, err = db03.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
}

func (ms *NullableOrderByMergerSuite) TearDownSuite() {
	_ = ms.db01.Close()
	_ = ms.db02.Close()
	_ = ms.db03.Close()
}

func (ms *NullableOrderByMergerSuite) TestRows_Nullable() {
	testcases := []struct {
		name         string
		rowsList     func() []*sql.Rows
		sortColumns  []SortColumn
		wantErr      error
		afterFunc    func()
		wantVal      []DistinctNullable
		DistinctCols []merger.ColumnInfo
	}{
		{
			name: "测试去重",
			rowsList: func() []*sql.Rows {
				db1InsertSql := []string{
					"insert into t1  (id,  name) values (1,  'zwl')",
					"insert into t1  (id, age, name) values (2, 10, 'zwl')",
					"insert into t1  (id, age, name) values (3, 10, 'xz')",
					"insert into t1  (id, age) values (4, 10)",
				}
				for _, sql := range db1InsertSql {
					_, err := ms.db01.ExecContext(context.Background(), sql)
					require.NoError(ms.T(), err)
				}
				db2InsertSql := []string{
					"insert into t1  (id,  name) values (5,  'zwl')",
					"insert into t1  (id, age, name) values (6, 10, 'zwl')",
				}
				for _, sql := range db2InsertSql {
					_, err := ms.db02.ExecContext(context.Background(), sql)
					require.NoError(ms.T(), err)
				}
				db3InsertSql := []string{
					"insert into t1  (id, name) values (7, 'zwl')",
					"insert into t1  (id, age) values (8, 5)",
					"insert into t1  (id, age,name) values (9, 10,'xz')",
				}
				for _, sql := range db3InsertSql {
					_, err := ms.db03.ExecContext(context.Background(), sql)
					require.NoError(ms.T(), err)
				}
				dbs := []*sql.DB{ms.db01, ms.db02, ms.db03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				query := "SELECT distinct `age`,`name` FROM `t1` order by age ASC,name DESC"
				for _, db := range dbs {
					rows, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, rows)
				}
				return rowsList
			},
			sortColumns: []SortColumn{
				NewSortColumn("age", utils.ASC),
			},
			DistinctCols: []merger.ColumnInfo{
				{
					Index: 0, Name: "age",
				},
				{
					Index: 1, Name: "name",
				},
			},
			afterFunc: func() {
				dbs := []*sql.DB{ms.db01, ms.db02, ms.db03}
				for _, db := range dbs {
					_, err := db.Exec("DELETE FROM t1;")
					require.NoError(ms.T(), err)
				}
			},
			wantVal: func() []DistinctNullable {
				return []DistinctNullable{
					{
						Age:  sql.NullInt64{Valid: false, Int64: 0},
						Name: sql.NullString{Valid: true, String: "zwl"},
					},
					{
						Age:  sql.NullInt64{Valid: true, Int64: 5},
						Name: sql.NullString{Valid: false, String: ""},
					},
					{
						Age:  sql.NullInt64{Valid: true, Int64: 10},
						Name: sql.NullString{Valid: false, String: ""},
					},
					{
						Age:  sql.NullInt64{Valid: true, Int64: 10},
						Name: sql.NullString{Valid: true, String: "xz"},
					},
					{
						Age:  sql.NullInt64{Valid: true, Int64: 10},
						Name: sql.NullString{Valid: true, String: "zwl"},
					},
				}
			}(),
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			sortCols, err := newSortColumns(tc.sortColumns...)
			require.NoError(t, err)
			merger, err := NewDistinctMerger(tc.DistinctCols, sortCols)
			require.NoError(t, err)
			rows, err := merger.Merge(context.Background(), tc.rowsList())
			require.NoError(t, err)
			res := make([]DistinctNullable, 0, len(tc.wantVal))
			for rows.Next() {
				nullT := DistinctNullable{}
				err := rows.Scan(&nullT.Age, &nullT.Name)
				require.NoError(ms.T(), err)
				res = append(res, nullT)
			}
			require.True(t, rows.(*DistinctRows).closed)
			assert.NoError(t, rows.Err())
			assert.Equal(t, tc.wantVal, res)
			tc.afterFunc()
		})
	}
}

type DistinctNullable struct {
	Age  sql.NullInt64
	Name sql.NullString
}
