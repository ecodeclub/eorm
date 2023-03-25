package limitmerger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	"github.com/ecodeclub/eorm/internal/merger/sortmerger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/multierr"
)

var (
	offsetMockErr error = errors.New("rows: MockOffsetErr")
	limitMockErr  error = errors.New("rows: MockLimitErr")
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

func (ms *MergerSuite) TestMerger_Merge() {
	testcases := []struct {
		name        string
		getMerger   func() (merger.Merger, error)
		GetRowsList func() []*sql.Rows
		wantErr     error
		ctx         func() (context.Context, context.CancelFunc)
		limit       int
		offset      int
	}{
		{
			name: "limitMerger里的Merger的Merge出错",
			getMerger: func() (merger.Merger, error) {
				return sortmerger.NewMerger(sortmerger.NewSortColumn[int]("id", sortmerger.ASC))
			},
			GetRowsList: func() []*sql.Rows {
				return []*sql.Rows{}
			},
			wantErr: errs.ErrMergerEmptyRows,
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			limit:  0,
			offset: 0,
		},
		{
			name: "Next offset个值时遇到报错",
			getMerger: func() (merger.Merger, error) {
				return sortmerger.NewMerger(sortmerger.NewSortColumn[int]("id", sortmerger.ASC))
			},
			GetRowsList: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(2, "bruce", "cn").RowError(1, offsetMockErr))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(5, "a", "cn").AddRow(7, "b", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantErr: offsetMockErr,
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			limit:  10,
			offset: 5,
		},
		{
			name: "offset的值超过返回的数据行数",
			getMerger: func() (merger.Merger, error) {
				return sortmerger.NewMerger(sortmerger.NewSortColumn[int]("id", sortmerger.ASC))
			},
			GetRowsList: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(2, "bruce", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(5, "a", "cn").AddRow(7, "b", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			limit:  10,
			offset: 10,
		},
		{
			name: "超时",
			getMerger: func() (merger.Merger, error) {
				return sortmerger.NewMerger(sortmerger.NewSortColumn[int]("id", sortmerger.ASC))
			},
			GetRowsList: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(2, "bruce", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(5, "a", "cn").AddRow(7, "b", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 0)
			},
			wantErr: context.DeadlineExceeded,
			limit:   5,
			offset:  0,
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger, err := tc.getMerger()
			limitMerger := NewMerger(merger, tc.offset, tc.limit)
			require.NoError(t, err)
			ctx, cancel := tc.ctx()
			rows, err := limitMerger.Merge(ctx, tc.GetRowsList())
			cancel()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			require.NotNil(t, rows)

		})
	}
}

func (ms *MergerSuite) TestMerger_NextAndScan() {
	testcases := []struct {
		name        string
		getMerger   func() (merger.Merger, error)
		GetRowsList func() []*sql.Rows
		wantVal     []TestModel
		limit       int
		offset      int
	}{
		{
			name: "limit的行数超过了返回的总行数，",
			getMerger: func() (merger.Merger, error) {
				return sortmerger.NewMerger(sortmerger.NewSortColumn[int]("id", sortmerger.ASC))
			},
			GetRowsList: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "a", "cn").AddRow(7, "b", "cn"))
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
					Id:      2,
					Name:    "a",
					Address: "cn",
				},
				{
					Id:      3,
					Name:    "alex",
					Address: "cn",
				},
				{
					Id:      4,
					Name:    "x",
					Address: "cn",
				},
				{
					Id:      5,
					Name:    "bruce",
					Address: "cn",
				},
				{
					Id:      7,
					Name:    "b",
					Address: "cn",
				},
			},
			limit:  100,
			offset: 1,
		},
		{
			name: "limit 行数小于返回的总行数",
			getMerger: func() (merger.Merger, error) {
				return sortmerger.NewMerger(sortmerger.NewSortColumn[int]("id", sortmerger.ASC))
			},
			GetRowsList: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "a", "cn").AddRow(7, "b", "cn"))
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
					Id:      2,
					Name:    "a",
					Address: "cn",
				},
				{
					Id:      3,
					Name:    "alex",
					Address: "cn",
				},
			},
			limit:  2,
			offset: 1,
		},
		{
			name: "offset超过sqlRows列表返回的总行数",
			getMerger: func() (merger.Merger, error) {
				return sortmerger.NewMerger(sortmerger.NewSortColumn[int]("id", sortmerger.ASC))
			},
			GetRowsList: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "a", "cn").AddRow(7, "b", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{},
			limit:   2,
			offset:  100,
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger, err := tc.getMerger()
			require.NoError(t, err)
			limitMerger := NewMerger(merger, tc.offset, tc.limit)
			rows, err := limitMerger.Merge(context.Background(), tc.GetRowsList())
			require.NoError(t, err)
			res := make([]TestModel, 0, len(tc.wantVal))
			for rows.Next() {
				var model TestModel
				err = rows.Scan(&model.Id, &model.Name, &model.Address)
				require.NoError(t, err)
				res = append(res, model)
			}
			require.True(t, rows.(*Rows).closed)
			require.NoError(t, rows.Err())
			assert.Equal(t, tc.wantVal, res)
		})
	}
}

func (ms *MergerSuite) TestRows_NextAndErr() {
	testcases := []struct {
		name        string
		getMerger   func() (merger.Merger, error)
		GetRowsList func() []*sql.Rows
		wantErr     error
		limit       int
		offset      int
	}{
		{
			name: "有sql.Rows返回错误",
			getMerger: func() (merger.Merger, error) {
				return sortmerger.NewMerger(sortmerger.NewSortColumn[int]("id", sortmerger.ASC))
			},
			GetRowsList: func() []*sql.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "a", "cn").AddRow(7, "b", "cn").RowError(1, limitMockErr))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]*sql.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			limit:   10,
			offset:  1,
			wantErr: limitMockErr,
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger, err := tc.getMerger()
			require.NoError(t, err)
			limitMerger := NewMerger(merger, tc.offset, tc.limit)
			rows, err := limitMerger.Merge(context.Background(), tc.GetRowsList())
			require.NoError(t, err)
			for rows.Next() {
			}
			require.True(t, rows.(*Rows).closed)
			assert.Equal(t, tc.wantErr, rows.Err())
		})
	}
}

func (ms *MergerSuite) TestRows_ScanAndErr() {
	ms.T().Run("未调用Next，直接Scan，返回错", func(t *testing.T) {
		cols := []string{"id"}
		query := "SELECT * FROM `t1`"
		ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1).AddRow(5))
		r, err := ms.mockDB01.QueryContext(context.Background(), query)
		require.NoError(t, err)
		rowsList := []*sql.Rows{r}
		merger, err := sortmerger.NewMerger(sortmerger.NewSortColumn[int]("id", sortmerger.ASC))
		require.NoError(t, err)
		limitMerger := NewMerger(merger, 0, 1)
		rows, err := limitMerger.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		id := 0
		err = rows.Scan(&id)
		require.Error(t, err)
	})
	ms.T().Run("迭代过程中发现错误,调用Scan，返回迭代中发现的错误", func(t *testing.T) {
		cols := []string{"id"}
		query := "SELECT * FROM `t1`"
		ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1).AddRow(2).RowError(1, limitMockErr))
		r, err := ms.mockDB01.QueryContext(context.Background(), query)
		require.NoError(t, err)
		rowsList := []*sql.Rows{r}
		merger, err := sortmerger.NewMerger(sortmerger.NewSortColumn[int]("id", sortmerger.ASC))
		require.NoError(t, err)
		limitMerger := NewMerger(merger, 0, 1)
		rows, err := limitMerger.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		for rows.Next() {
		}
		id := 0
		err = rows.Scan(&id)
		assert.Equal(t, limitMockErr, err)
	})
}

func (ms *MergerSuite) TestRows_Close() {
	cols := []string{"id"}
	query := "SELECT * FROM `t1`"
	ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("1"))
	ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("2").AddRow("5").CloseError(newCloseMockErr("db02")))
	ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4").CloseError(newCloseMockErr("db03")))
	merger, err := sortmerger.NewMerger(sortmerger.NewSortColumn[int]("id", sortmerger.ASC))
	require.NoError(ms.T(), err)
	limitMerger := NewMerger(merger, 1, 6)
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]*sql.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), query)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}
	rows, err := limitMerger.Merge(context.Background(), rowsList)
	require.NoError(ms.T(), err)
	// 判断当前是可以正常读取的
	require.True(ms.T(), rows.Next())
	var id int
	err = rows.Scan(&id)
	require.NoError(ms.T(), err)
	err = rows.Close()
	ms.T().Run("close返回error", func(t *testing.T) {
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
	cols := []string{"id"}
	query := "SELECT * FROM `t1`"
	ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("1"))
	ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("2"))
	ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4"))
	merger, err := sortmerger.NewMerger(sortmerger.NewSortColumn[int]("id", sortmerger.ASC))
	limitMerger := NewMerger(merger, 0, 10)
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]*sql.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), query)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}
	rows, err := limitMerger.Merge(context.Background(), rowsList)
	columns, err := rows.Columns()
	require.NoError(ms.T(), err)
	assert.Equal(ms.T(), cols, columns)
}

func TestMerger(t *testing.T) {
	suite.Run(t, &MergerSuite{})
}

type TestModel struct {
	Id      int
	Name    string
	Address string
}
