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

package aggregatemerger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/merger/aggregatemerger/aggregator"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/multierr"
	"testing"
)

var (
	nextMockErr error = errors.New("rows: MockNextErr")
)

func newCloseMockErr(dbName string) error {
	return fmt.Errorf("rows: %s MockCloseErr", dbName)
}

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
	db05     *sql.DB
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
	_ = ms.db05.Close()
}

func (ms *MergerSuite) initMock(t *testing.T) {
	var err error
	query := "CREATE TABLE t1" +
		"(" +
		"   id INT PRIMARY KEY     NOT NULL," +
		"   grade            INT     NOT NULL" +
		");"
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
	db05, err := sql.Open("sqlite3", "file:test01.db?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}
	ms.db05 = db05
	_, err = db05.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMerger(t *testing.T) {
	suite.Run(t, &MergerSuite{})
}

func (ms *MergerSuite) TestRows_NextAndScan() {
	testcases := []struct {
		name        string
		sqlRows     func() []*sql.Rows
		wantVal     []any
		aggregators func() []aggregator.Aggregator
		res         []any
		wantErr     error
	}{
		{
			name: "rowList中含有sqlLite",
			sqlRows: func() []*sql.Rows {
				cols := []string{"SUM(id)"}
				query := "SELECT SUM(`id`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
				query1 := "insert into `t1` values (1,10),(2,20),(3,30)"
				_, err := ms.db05.ExecContext(context.Background(), query1)
				require.NoError(ms.T(), err)
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs)+1)
				rows, err := ms.db05.QueryContext(context.Background(), query)
				require.NoError(ms.T(), err)
				rowsList = append(rowsList, rows)
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{int64(66)},
			res: func() []any {
				return []any{
					0,
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewSum(aggregator.NewColumnInfo(0, "SUM(id)")),
				}
			},
		},
		{
			name: "单个SUM",
			sqlRows: func() []*sql.Rows {
				cols := []string{"SUM(id)"}
				query := "SELECT SUM(`id`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{int64(60)},
			res: func() []any {
				return []any{
					0,
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewSum(aggregator.NewColumnInfo(0, "SUM(id)")),
				}
			},
		},

		{
			name: "单个MAX",
			sqlRows: func() []*sql.Rows {
				cols := []string{"MAX(id)"}
				query := "SELECT MAX(`id`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{int64(30)},
			res: func() []any {
				return []any{
					0,
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewMax(aggregator.NewColumnInfo(0, "MAX(id)")),
				}
			},
		},
		{
			name: "单个MIN",
			sqlRows: func() []*sql.Rows {
				cols := []string{"MIN(id)"}
				query := "SELECT MIN(`id`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{int64(10)},
			res: func() []any {
				return []any{
					0,
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewMin(aggregator.NewColumnInfo(0, "MIN(id)")),
				}
			},
		},
		{
			name: "单个count",
			sqlRows: func() []*sql.Rows {
				cols := []string{"COUNT(id)"}
				query := "SELECT COUNT(`id`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{int64(40)},
			res: func() []any {
				return []any{
					0,
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewCount(aggregator.NewColumnInfo(0, "COUNT(id)")),
				}
			},
		},
		{
			name: "AVG",
			sqlRows: func() []*sql.Rows {
				cols := []string{"SUM(grade)", "COUNT(grade)"}
				query := "SELECT SUM(`grade`),COUNT(`grade`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2000, 10))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2000, 20))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2000, 10))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{
				float64(150),
			},
			res: func() []any {
				return []any{
					float64(0),
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewAVG(aggregator.NewColumnInfo(0, "SUM(grade)"), aggregator.NewColumnInfo(1, "COUNT(grade)"), "AVG(grade)"),
				}
			},
		},
		// 下面为多个聚合函数组合的情况

		// 1.每种聚合函数出现一次
		{
			name: "COUNT,MAX,MIN,SUM,AVG",
			sqlRows: func() []*sql.Rows {
				cols := []string{"COUNT(id)", "MAX(id)", "MIN(id)", "SUM(id)", "SUM(grade)", "COUNT(grade)"}
				query := "SELECT COUNT(`id`),MAX(`id`),MIN(`id`),SUM(`id`),SUM(`grade`),COUNT(`student`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(10, 20, 1, 100, 2000, 20))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(20, 30, 0, 200, 800, 10))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(10, 40, 2, 300, 1800, 20))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{
				int64(40), int64(40), int64(0), int64(600), float64(4600) / float64(50),
			},
			res: func() []any {
				return []any{
					0, 0, 0, 0, float64(0),
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewCount(aggregator.NewColumnInfo(0, "COUNT(id)")),
					aggregator.NewMax(aggregator.NewColumnInfo(1, "MAX(id)")),
					aggregator.NewMin(aggregator.NewColumnInfo(2, "MIN(id)")),
					aggregator.NewSum(aggregator.NewColumnInfo(3, "SUM(id)")),
					aggregator.NewAVG(aggregator.NewColumnInfo(4, "SUM(grade)"), aggregator.NewColumnInfo(5, "COUNT(grade)"), "AVG(grade)"),
				}
			},
		},
		// 2. 每种聚合函数出现多次，会有相同的聚合函数类型，且相同的聚合函数类型会有连续出现，和不连续出现。两个avg会包含sum列在前，和sum列在后的状态。并且有完全相同的列出现
		{
			name: "avg,sum,avg,min,min,max,count",
			sqlRows: func() []*sql.Rows {
				cols := []string{"SUM(grade)", "COUNT(grade)", "SUM(grade)", "COUNT(grade)", "SUM(grade)", "MIN(id)", "MIN(userid)", "MAX(id)", "COUNT(id)"}
				query := "SELECT SUM(`grade`),COUNT(`grade`),SUM(`grade`),COUNT(`grade`),SUM(`grade`),MIN(`id`),MIN(`userid`),MAX(`id`),COUNT(`id`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2000, 20, 2000, 20, 2000, 10, 20, 200, 200))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1000, 10, 1000, 10, 1000, 20, 30, 300, 300))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(800, 10, 800, 10, 800, 5, 6, 100, 200))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{
				float64(3800) / float64(40), int64(3800), float64(3800) / float64(40), int64(5), int64(6), int64(300), int64(700),
			},
			res: func() []any {
				return []any{
					float64(0), 0, float64(0), 0, 0, 0, 0,
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewAVG(aggregator.NewColumnInfo(0, "SUM(grade)"), aggregator.NewColumnInfo(1, "COUNT(grade)"), "AVG(grade)"),
					aggregator.NewSum(aggregator.NewColumnInfo(2, "SUM(grade)")),
					aggregator.NewAVG(aggregator.NewColumnInfo(4, "SUM(grade)"), aggregator.NewColumnInfo(3, "COUNT(grade)"), "AVG(grade)"),
					aggregator.NewMin(aggregator.NewColumnInfo(5, "MIN(id)")),
					aggregator.NewMin(aggregator.NewColumnInfo(6, "MIN(userid)")),
					aggregator.NewMax(aggregator.NewColumnInfo(7, "MAX(id)")),
					aggregator.NewCount(aggregator.NewColumnInfo(8, "COUNT(id)")),
				}
			},
		},

		// 下面为RowList为有元素返回的行数为空

		// 1. Rows 列表中有一个Rows返回行数为空，在前面会返回错误
		{
			name: "RowsList有一个Rows为空，在前面",
			sqlRows: func() []*sql.Rows {
				cols := []string{"SUM(id)"}
				query := "SELECT SUM(`id`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
				ms.mock04.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols))
				dbs := []*sql.DB{ms.mockDB04, ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{60},
			res: func() []any {
				return []any{
					0,
				}
			}(),
			wantErr: errs.ErrMergerAggregateHasEmptyRows,
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewSum(aggregator.NewColumnInfo(0, "SUM(id)")),
				}
			},
		},
		// 2. Rows 列表中有一个Rows返回行数为空，在中间会返回错误
		{
			name: "RowsList有一个Rows为空，在中间",
			sqlRows: func() []*sql.Rows {
				cols := []string{"SUM(id)"}
				query := "SELECT SUM(`id`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
				ms.mock04.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB04, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{60},
			res: func() []any {
				return []any{
					0,
				}
			}(),
			wantErr: errs.ErrMergerAggregateHasEmptyRows,
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewSum(aggregator.NewColumnInfo(0, "SUM(id)")),
				}
			},
		},
		// 3. Rows 列表中有一个Rows返回行数为空，在后面会返回错误
		{
			name: "RowsList有一个Rows为空，在最后",
			sqlRows: func() []*sql.Rows {
				cols := []string{"SUM(id)"}
				query := "SELECT SUM(`id`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
				ms.mock04.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03, ms.mockDB04}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{60},
			res: func() []any {
				return []any{
					0,
				}
			}(),
			wantErr: errs.ErrMergerAggregateHasEmptyRows,
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewSum(aggregator.NewColumnInfo(0, "SUM(id)")),
				}
			},
		},
		// 4. Rows 列表中全部Rows返回的行数为空，不会返回错误
		{
			name: "RowsList全部为空",
			sqlRows: func() []*sql.Rows {
				cols := []string{"SUM(id)"}
				query := "SELECT SUM(`id`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols))
				ms.mock04.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03, ms.mockDB04}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantErr: errs.ErrMergerAggregateHasEmptyRows,
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewSum(aggregator.NewColumnInfo(0, "SUM(id)")),
				}
			},
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			m := NewMerger(tc.aggregators()...)
			rows, err := m.Merge(context.Background(), tc.sqlRows())
			require.NoError(t, err)
			for rows.Next() {
				kk := make([]any, 0, len(tc.res))
				for i := 0; i < len(tc.res); i++ {
					kk = append(kk, &tc.res[i])
				}
				err = rows.Scan(kk...)
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantErr, rows.Err())
			if rows.Err() != nil {
				return
			}
			assert.Equal(t, tc.wantVal, tc.res)
		})
	}
}

func (ms *MergerSuite) TestRows_NextAndErr() {
	testcases := []struct {
		name        string
		rowsList    func() []*sql.Rows
		wantErr     error
		aggregators []aggregator.Aggregator
	}{
		{
			name: "sqlRows列表中有一个返回error",
			rowsList: func() []*sql.Rows {
				cols := []string{"COUNT(id)"}
				query := "SELECT COUNT(`id`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(4).RowError(0, nextMockErr))
				ms.mock04.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(5))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03, ms.mockDB04}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewCount(aggregator.NewColumnInfo(0, "COUNT(id)")),
				}
			}(),
			wantErr: nextMockErr,
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger := NewMerger(tc.aggregators...)
			rows, err := merger.Merge(context.Background(), tc.rowsList())
			require.NoError(t, err)
			for rows.Next() {
			}
			assert.Equal(t, tc.wantErr, rows.Err())
		})
	}
}

func (ms *MergerSuite) TestRows_Close() {
	cols := []string{"SUM(id)"}
	query := "SELECT SUM(`id`) FROM `t1`"
	ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1))
	ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2).CloseError(newCloseMockErr("db02")))
	ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(3).CloseError(newCloseMockErr("db03")))
	merger := NewMerger(aggregator.NewSum(aggregator.NewColumnInfo(0, "SUM(id)")))
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

func (ms *MergerSuite) TestRows_Columns() {
	cols := []string{"SUM(grade)", "COUNT(grade)"}
	query := "SELECT SUM(`grade`),COUNT(`grade`) FROM `t1`"
	ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 1))
	ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2, 1))
	ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(3, 1))
	merger := NewMerger(aggregator.NewAVG(aggregator.NewColumnInfo(0, "SUM(grade)"), aggregator.NewColumnInfo(1, "COUNT(grade)"), "AVG(grade)"))
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]*sql.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), query)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}

	rows, err := merger.Merge(context.Background(), rowsList)
	require.NoError(ms.T(), err)
	wantCols := []string{"AVG(grade)"}
	ms.T().Run("Next没有迭代完", func(t *testing.T) {
		for rows.Next() {
			columns, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, wantCols, columns)
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

func (ms *MergerSuite) TestMerger_Merge() {
	testcases := []struct {
		name    string
		merger  func() *Merger
		ctx     func() (context.Context, context.CancelFunc)
		wantErr error
		sqlRows func() []*sql.Rows
	}{
		{
			name: "超时",
			merger: func() *Merger {
				return NewMerger(aggregator.NewSum(aggregator.NewColumnInfo(0, "SUM(id)")))
			},
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(context.Background(), 0)
				return ctx, cancel
			},
			wantErr: context.DeadlineExceeded,
			sqlRows: func() []*sql.Rows {
				query := "SELECT  SUM(`id`) FROM `t1`;"
				cols := []string{"SUM(id)"}
				res := make([]*sql.Rows, 0, 1)
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1))
				rows, _ := ms.mockDB01.QueryContext(context.Background(), query)
				res = append(res, rows)
				return res
			},
		},
		{
			name: "sqlRows列表元素个数为0",
			merger: func() *Merger {
				return NewMerger(aggregator.NewSum(aggregator.NewColumnInfo(0, "SUM(id)")))
			},
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel
			},
			wantErr: errs.ErrMergerEmptyRows,
			sqlRows: func() []*sql.Rows {
				return []*sql.Rows{}
			},
		},
		{
			name: "sqlRows列表有nil",
			merger: func() *Merger {
				return NewMerger(aggregator.NewSum(aggregator.NewColumnInfo(0, "SUM(id)")))
			},
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel
			},
			wantErr: errs.ErrMergerRowsIsNull,
			sqlRows: func() []*sql.Rows {
				return []*sql.Rows{nil}
			},
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			ctx, cancel := tc.ctx()
			m := tc.merger()
			r, err := m.Merge(ctx, tc.sqlRows())
			cancel()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			require.NotNil(t, r)
		})
	}
}
