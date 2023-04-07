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

package single

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/ecodeclub/eorm/internal/datasource"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type SingleSuite struct {
	suite.Suite
	mockDB *sql.DB
	mock   sqlmock.Sqlmock
}

func (s *SingleSuite) SetupTest() {
	t := s.T()
	s.initMock(t)
}

func (s *SingleSuite) TearDownTest() {
	_ = s.mockDB.Close()
}

func (s *SingleSuite) initMock(t *testing.T) {
	var err error
	s.mockDB, s.mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
}

func (s *SingleSuite) TestDBQuery() {
	//s.mock.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("value"))

	testCases := []struct {
		name     string
		query    datasource.Query
		mockRows *sqlmock.Rows
		wantResp []string
		wantErr  error
	}{
		{
			name: "one row",
			query: datasource.Query{
				SQL: "SELECT `first_name` FROM `test_model`",
			},
			mockRows: sqlmock.NewRows([]string{"first_name"}).AddRow("value"),
			wantResp: []string{"value"},
		},
		{
			name: "multi row",
			query: datasource.Query{
				SQL: "SELECT `first_name` FROM `test_model`",
			},
			mockRows: func() *sqlmock.Rows {
				res := sqlmock.NewRows([]string{"first_name"})
				res.AddRow("value1")
				res.AddRow("value2")
				return res
			}(),
			wantResp: []string{"value1", "value2"},
		},
	}
	for _, tc := range testCases {
		s.mock.ExpectQuery(tc.query.SQL).WillReturnRows(tc.mockRows)
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			db := NewDB(s.mockDB)
			rows, queryErr := db.Query(context.Background(), tc.query)
			assert.Equal(t, queryErr, tc.wantErr)
			if queryErr != nil {
				return
			}
			assert.NotNil(t, rows)
			var resp []string
			for rows.Next() {
				val := new(string)
				err := rows.Scan(val)
				assert.Nil(t, err)
				if err != nil {
					return
				}
				assert.NotNil(t, val)
				resp = append(resp, *val)
			}

			assert.ElementsMatch(t, tc.wantResp, resp)
		})
	}
}

func (s *SingleSuite) TestDBExec() {
	testCases := []struct {
		name         string
		lastInsertId int64
		rowsAffected int64
		wantErr      error
		mockResult   driver.Result
		query        datasource.Query
	}{
		{
			name: "res 1",
			query: datasource.Query{
				SQL: "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4)",
			},
			mockResult: func() driver.Result {
				return sqlmock.NewResult(2, 1)
			}(),
			lastInsertId: int64(2),
			rowsAffected: int64(1),
		},
		{
			name: "res 2",
			query: datasource.Query{
				SQL: "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4) (1,2,3,4)",
			},
			mockResult: func() driver.Result {
				return sqlmock.NewResult(4, 2)
			}(),
			lastInsertId: int64(4),
			rowsAffected: int64(2),
		},
	}
	for _, tc := range testCases {
		s.mock.ExpectExec("^INSERT INTO (.+)").WillReturnResult(tc.mockResult)
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			db := NewDB(s.mockDB)
			res, err := db.Exec(context.Background(), tc.query)
			assert.Nil(t, err)
			lastInsertId, err := res.LastInsertId()
			assert.Nil(t, err)
			assert.EqualValues(t, tc.lastInsertId, lastInsertId)
			rowsAffected, err := res.RowsAffected()
			assert.Nil(t, err)
			assert.EqualValues(t, tc.rowsAffected, rowsAffected)
		})
	}
}

func TestSingleSuite(t *testing.T) {
	suite.Run(t, &SingleSuite{})
}

func TestDB_BeginTx(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()

	db := NewDB(mockDB)
	// Begin 失败
	mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
	assert.Equal(t, errors.New("begin failed"), err)
	assert.Nil(t, tx)

	mock.ExpectBegin()
	tx, err = db.BeginTx(context.Background(), &sql.TxOptions{})
	assert.Nil(t, err)
	assert.NotNil(t, tx)
}

func TestDB_Wait(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()

	db := NewDB(mockDB)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectPing()
	err = db.Wait()
	assert.Nil(t, err)
}

func ExampleDB_BeginTx() {
	db, _ := OpenDB("sqlite3", "file:test.db?cache=shared&mode=memory")
	defer func() {
		_ = db.Close()
	}()
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
	if err == nil {
		fmt.Println("Begin")
	}
	// 或者 tx.Rollback()
	err = tx.Commit()
	if err == nil {
		fmt.Println("Commit")
	}
	// Output:
	// Begin
	// Commit
}

func ExampleDB_Close() {
	db, _ := OpenDB("sqlite3", "file:test.db?cache=shared&mode=memory")
	err := db.Close()
	if err == nil {
		fmt.Println("close")
	}

	// Output:
	// close
}
