package sortmerger

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	"github.com/ecodeclub/eorm/internal/merger/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

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
			name: "排序列不在去重列中",
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
			m, err := NewDistinctMerger(tc.sortCols, tc.distinctCols)
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
				return NewDistinctMerger(sortcols, []merger.ColumnInfo{merger.NewColumnInfo(0, "id"), merger.NewColumnInfo(2, "name"), merger.NewColumnInfo(3, "address")})
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
				return NewDistinctMerger(sortcols, []merger.ColumnInfo{merger.NewColumnInfo(0, "id"), merger.NewColumnInfo(2, "name"), merger.NewColumnInfo(3, "address")})

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
				return NewDistinctMerger(sortcols, []merger.ColumnInfo{merger.NewColumnInfo(0, "id"), merger.NewColumnInfo(2, "name"), merger.NewColumnInfo(3, "address")})

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
				return NewDistinctMerger(sortcols, []merger.ColumnInfo{merger.NewColumnInfo(0, "id"), merger.NewColumnInfo(2, "name"), merger.NewColumnInfo(3, "address")})
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
				return NewDistinctMerger(sortcols, []merger.ColumnInfo{merger.NewColumnInfo(0, "id"), merger.NewColumnInfo(2, "name"), merger.NewColumnInfo(3, "address")})
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
				return NewDistinctMerger(sortcols, []merger.ColumnInfo{merger.NewColumnInfo(0, "id"), merger.NewColumnInfo(1, "name"), merger.NewColumnInfo(2, "address")})
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
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			m, err := NewDistinctMerger(tc.sortColumns, tc.distinctColumns)
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

func TestOrderbyMerger(t *testing.T) {
	suite.Run(t, &DistinctMergerSuite{})

}
