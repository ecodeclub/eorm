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
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gotomicro/eorm/internal/errs"
	"github.com/gotomicro/eorm/internal/valuer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type QuerierTestSuite struct {
	suite.Suite
	orm *DB
}

func TestQuerier(t *testing.T) {
	suite.Run(t, &QuerierTestSuite{})
}

func (q *QuerierTestSuite) SetupSuite() {
	q.orm = memoryDBWithDB("querier")
	// 创建表
	r := RawQuery[TestModel](q.orm, TestModel{}.CreateSQL()).Exec(context.Background())
	if r.Err() != nil {
		q.T().Fatal(r.Err())
	}

	// 准备数据
	res := NewInserter[TestModel](q.orm).Values(&TestModel{
		Id:        1,
		FirstName: "Tom",
		Age:       18,
		LastName:  &sql.NullString{String: "Jerry", Valid: true},
	}).Exec(context.Background())

	if res.Err() != nil {
		q.T().Fatal(res.Err())
	}
}

func (q *QuerierTestSuite) TestExec() {
	t := q.T()
	query := RawQuery[int](q.orm, `CREATE TABLE groups (
   group_id INTEGER PRIMARY KEY,
   name TEXT NOT NULL
)`)
	res := query.Exec(context.Background())
	if res.Err() != nil {
		t.Fatal(res.Err())
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int64(1), affected)
}

func (q *QuerierTestSuite) TestGet() {
	t := q.T()
	testCases := []struct {
		name       string
		s          *Selector[TestModel]
		wantErr    error
		wantResult *TestModel
	}{
		{
			name:    "not found",
			s:       NewSelector[TestModel](q.orm).From(&TestModel{}).Where(C("Id").EQ(12)),
			wantErr: errs.ErrNoRows,
		},
		{
			name: "found",
			s:    NewSelector[TestModel](q.orm).From(&TestModel{}).Where(C("Id").EQ(1)),
			wantResult: &TestModel{
				Id:        1,
				FirstName: "Tom",
				Age:       18,
				LastName:  &sql.NullString{String: "Jerry", Valid: true},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tc.s.Get(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantResult, res)
		})
	}
}

func ExampleRawQuery() {
	orm := memoryDB()
	q := RawQuery[any](orm, `SELECT * FROM user_tab WHERE id = ?;`, 1)
	fmt.Printf(`
SQL: %s
Args: %v
`, q.q.SQL, q.q.Args)
	// Output:
	// SQL: SELECT * FROM user_tab WHERE id = ?;
	// Args: [1]
}

func ExampleQuerier_Exec() {
	orm := memoryDB()
	// 在 Exec 的时候，泛型参数可以是任意的
	q := RawQuery[any](orm, `CREATE TABLE IF NOT EXISTS groups (
   group_id INTEGER PRIMARY KEY,
   name TEXT NOT NULL
)`)
	res := q.Exec(context.Background())
	if res.Err() == nil {
		fmt.Print("SUCCESS")
	}
	// Output:
	// SUCCESS
}

func (q Query) string() string {
	return fmt.Sprintf("SQL: %s\nArgs: %#v\n", q.SQL, q.Args)
}

func TestQuerier_Get(t *testing.T) {
	t.Run("unsafe", func(t *testing.T) {
		testQuerierGet(t, valuer.NewUnsafeValue)
	})

	t.Run("reflect", func(t *testing.T) {
		testQuerierGet(t, valuer.NewReflectValue)
	})
}

func testQuerierGet(t *testing.T, c valuer.Creator) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	orm, err := openDB("mysql", db)
	if err != nil {
		t.Fatal(err)
	}
	testCases := []struct {
		name     string
		query    string
		mockErr  error
		mockRows *sqlmock.Rows
		wantErr  error
		wantVal  *TestModel
	}{
		{
			// 查询返回错误
			name:    "query error",
			mockErr: errors.New("invalid query"),
			wantErr: errors.New("invalid query"),
			query:   "invalid query",
		},
		{
			name:     "no row",
			wantErr:  ErrNoRows,
			query:    "no row",
			mockRows: sqlmock.NewRows([]string{"id"}),
		},
		{
			name:    "too many column",
			wantErr: errs.ErrTooManyColumns,
			query:   "too many column",
			mockRows: func() *sqlmock.Rows {
				res := sqlmock.NewRows([]string{"id", "first_name", "age", "last_name", "extra_column"})
				res.AddRow([]byte("1"), []byte("Da"), []byte("18"), []byte("Ming"), []byte("nothing"))
				return res
			}(),
		},
		{
			name:  "get data",
			query: "SELECT xx FROM `test_model`",
			mockRows: func() *sqlmock.Rows {
				res := sqlmock.NewRows([]string{"id", "first_name", "age", "last_name"})
				res.AddRow([]byte("1"), []byte("Da"), []byte("18"), []byte("Ming"))
				return res
			}(),
			wantVal: &TestModel{
				Id:        1,
				FirstName: "Da",
				Age:       18,
				LastName:  &sql.NullString{String: "Ming", Valid: true},
			},
		},
	}

	for _, tc := range testCases {
		exp := mock.ExpectQuery(tc.query)
		if tc.mockErr != nil {
			exp.WillReturnError(tc.mockErr)
		} else {
			exp.WillReturnRows(tc.mockRows)
		}
	}

	orm.valCreator = c
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := RawQuery[TestModel](orm, tc.query).Get(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, res)
		})
	}
}

type QuerierTestSuiteMulti struct {
	suite.Suite
	orm *DB
}

func Test_GetMulti(t *testing.T) {
	suite.Run(t, &QuerierTestSuiteMulti{})
}

func (q *QuerierTestSuiteMulti) SetupSuite() {
	q.orm = memoryDBWithDB("querier_multi")
	// 创建表
	r := RawQuery[TestModel](q.orm, TestModel{}.CreateSQL()).Exec(context.Background())
	if r.Err() != nil {
		q.T().Fatal(r.Err())
	}
	// 准备数据
	res := NewInserter[TestModel](q.orm).Values(
		&TestModel{
			Id:        1,
			FirstName: "Jack",
			Age:       20,
			LastName:  &sql.NullString{String: "Rose", Valid: true},
		}, &TestModel{
			Id:        2,
			FirstName: "Tom",
			Age:       18,
			LastName:  &sql.NullString{String: "Jerry", Valid: true},
		}).Exec(context.Background())

	if res.Err() != nil {
		q.T().Fatal(res.Err())
	}

}

func (q *QuerierTestSuiteMulti) TestGetMulti() {
	t := q.T()
	testCases := []struct {
		name       string
		s          *Selector[TestModel]
		wantErr    error
		wantResult []*TestModel
	}{
		{
			name:       "not found",
			s:          NewSelector[TestModel](q.orm).From(&TestModel{}).Where(C("Id").EQ(12)),
			wantResult: []*TestModel{},
		},
		{
			name: "found",
			s:    NewSelector[TestModel](q.orm).From(&TestModel{}).Where(C("Id").LT(4)),
			wantResult: []*TestModel{
				&TestModel{
					Id:        1,
					FirstName: "Jack",
					Age:       20,
					LastName:  &sql.NullString{String: "Rose", Valid: true},
				},
				&TestModel{
					Id:        2,
					FirstName: "Tom",
					Age:       18,
					LastName:  &sql.NullString{String: "Jerry", Valid: true},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tc.s.GetMulti(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantResult, res)
		})
	}
}

func TestQuerierGetMulti(t *testing.T) {
	t.Run("unsafe", func(t *testing.T) {
		testQuerierGetMulti(t, valuer.NewUnsafeValue)
	})
	t.Run("reflect", func(t *testing.T) {
		testQuerierGetMulti(t, valuer.NewUnsafeValue)
	})
}

func testQuerierGetMulti(t *testing.T, c valuer.Creator) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()
	orm, err := openDB("mysql", db)
	if err != nil {
		t.Fatal(err)
	}
	testCases := []struct {
		name     string
		query    string
		mockErr  error
		mockRows *sqlmock.Rows
		wantErr  error
		wantVal  []*TestModel
	}{
		{
			name:    "query error",
			mockErr: errors.New("invalid query"),
			wantErr: errors.New("invalid query"),
			query:   "invalid query",
		},
		{
			name:     "no row",
			query:    "no row",
			mockRows: sqlmock.NewRows([]string{"id"}),
			wantVal:  []*TestModel{},
		},
		{
			name:    "too many column",
			wantErr: errs.ErrTooManyColumns,
			query:   "too many column",
			mockRows: func() *sqlmock.Rows {
				res := sqlmock.NewRows([]string{"id", "first_name", "age", "last_name", "extra_column"})
				res.AddRow([]byte("1"), []byte("Da"), []byte("18"), []byte("Ming"), []byte("nothing"))
				return res
			}(),
		},
		{
			name:  "get data",
			query: "SELECT xx FROM `test_model`",
			mockRows: func() *sqlmock.Rows {
				res := sqlmock.NewRows([]string{"id", "first_name", "age", "last_name"})
				res.AddRow([]byte("1"), []byte("Da"), []byte("18"), []byte("Ming"))
				res.AddRow([]byte("2"), []byte("Xiao"), []byte("28"), []byte("Hong"))
				return res
			}(),
			wantVal: []*TestModel{&TestModel{
				Id:        1,
				FirstName: "Da",
				Age:       18,
				LastName:  &sql.NullString{String: "Ming", Valid: true},
			},
				{
					Id:        2,
					FirstName: "Xiao",
					Age:       28,
					LastName:  &sql.NullString{String: "Hong", Valid: true},
				},
			},
		},
	}
	for _, tc := range testCases {
		exp := mock.ExpectQuery(tc.query)
		if tc.mockErr != nil {
			exp.WillReturnError(tc.mockErr)
		} else {
			exp.WillReturnRows(tc.mockRows)
		}
	}
	orm.valCreator = c
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := RawQuery[TestModel](orm, tc.query).GetMulti(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, res)
		})
	}

}
