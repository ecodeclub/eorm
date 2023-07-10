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

package transaction_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ecodeclub/eorm/internal/datasource/transaction"

	"github.com/stretchr/testify/suite"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/stretchr/testify/assert"
)

func TestTx_Commit(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()

	db := openMockDB("mysql", mockDB)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		mock.ExpectClose()
		_ = db.Close()
	}()

	// 事务正常提交
	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
	assert.Nil(t, err)
	err = tx.Commit()
	assert.Nil(t, err)

}

func TestTx_Rollback(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()

	db := openMockDB("mysql", mockDB)
	if err != nil {
		t.Fatal(err)
	}

	// 事务回滚
	mock.ExpectBegin()
	mock.ExpectRollback()
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
	assert.Nil(t, err)
	err = tx.Rollback()
	assert.Nil(t, err)
}

type testMockDB struct {
	driver string
	db     *sql.DB
}

func (*testMockDB) Query(_ context.Context, _ datasource.Query) (*sql.Rows, error) {
	return &sql.Rows{}, nil
}

func (*testMockDB) Exec(_ context.Context, _ datasource.Query) (sql.Result, error) {
	return nil, nil
}

func openMockDB(driver string, db *sql.DB) *testMockDB {
	return &testMockDB{driver: driver, db: db}
}

func (db *testMockDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (datasource.Tx, error) {
	tx, err := db.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return transaction.NewTx(tx, db), nil
}

func (db *testMockDB) Close() error {
	return db.db.Close()
}

type TransactionSuite struct {
	suite.Suite
	mockDB1 *sql.DB
	mock1   sqlmock.Sqlmock

	mockDB2 *sql.DB
	mock2   sqlmock.Sqlmock

	mockDB3 *sql.DB
	mock3   sqlmock.Sqlmock
}

func (s *TransactionSuite) SetupTest() {
	t := s.T()
	s.initMock(t)
}

func (s *TransactionSuite) TearDownTest() {
	_ = s.mockDB1.Close()
	_ = s.mockDB2.Close()
	_ = s.mockDB3.Close()
}

func (s *TransactionSuite) initMock(t *testing.T) {
	var err error
	s.mockDB1, s.mock1, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockDB2, s.mock2, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockDB3, s.mock3, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
}

func (s *TransactionSuite) TestDBQuery() {
	//s.mock.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("value"))
	testCases := []struct {
		name     string
		tx       *transaction.Tx
		query    datasource.Query
		mockRows *sqlmock.Rows
		wantResp []string
		wantErr  error
	}{
		{
			name: "query tx",
			query: datasource.Query{
				SQL: "SELECT `first_name` FROM `test_model`",
			},
			tx: func() *transaction.Tx {
				s.mock1.ExpectBegin()
				s.mock1.ExpectQuery("SELECT *").WillReturnRows(
					sqlmock.NewRows([]string{"first_name"}).AddRow("value"))
				s.mock1.ExpectCommit()
				tx, err := s.mockDB1.BeginTx(context.Background(), &sql.TxOptions{})
				assert.Nil(s.T(), err)
				return transaction.NewTx(tx, NewMockDB(s.mockDB1))
			}(),
			wantResp: []string{"value"},
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			tx := tc.tx
			rows, queryErr := tx.Query(context.Background(), tc.query)
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
			assert.Nil(t, tx.Commit())
			assert.ElementsMatch(t, tc.wantResp, resp)
		})
	}
}

func (s *TransactionSuite) TestDBExec() {
	testCases := []struct {
		name         string
		lastInsertId int64
		rowsAffected int64
		wantErr      error
		isCommit     bool
		tx           *transaction.Tx
		query        datasource.Query
	}{
		{
			name: "res 1 rollback",
			query: datasource.Query{
				SQL: "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4)",
			},
			tx: func() *transaction.Tx {
				s.mock1.ExpectBegin()
				s.mock1.ExpectExec("^INSERT INTO (.+)").
					WillReturnResult(sqlmock.NewResult(2, 1))
				s.mock1.ExpectRollback()
				tx, err := s.mockDB1.BeginTx(context.Background(), &sql.TxOptions{})
				assert.Nil(s.T(), err)
				return transaction.NewTx(tx, NewMockDB(s.mockDB1))
			}(),
			lastInsertId: int64(2),
			rowsAffected: int64(1),
		},
		{
			name: "res 1",
			query: datasource.Query{
				SQL: "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4)",
			},
			tx: func() *transaction.Tx {
				s.mock2.ExpectBegin()
				s.mock2.ExpectExec("^INSERT INTO (.+)").
					WillReturnResult(sqlmock.NewResult(2, 1))
				s.mock2.ExpectCommit()
				tx, err := s.mockDB2.BeginTx(context.Background(), &sql.TxOptions{})
				assert.Nil(s.T(), err)
				return transaction.NewTx(tx, NewMockDB(s.mockDB2))
			}(),
			isCommit:     true,
			lastInsertId: int64(2),
			rowsAffected: int64(1),
		},
		{
			name: "res 2",
			query: datasource.Query{
				SQL: "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4) (1,2,3,4)",
			},
			tx: func() *transaction.Tx {
				s.mock3.ExpectBegin()
				s.mock3.ExpectExec("^INSERT INTO (.+)").
					WillReturnResult(sqlmock.NewResult(4, 2))
				s.mock3.ExpectCommit()
				tx, err := s.mockDB3.BeginTx(context.Background(), &sql.TxOptions{})
				assert.Nil(s.T(), err)
				return transaction.NewTx(tx, NewMockDB(s.mockDB3))
			}(),
			isCommit:     true,
			lastInsertId: int64(4),
			rowsAffected: int64(2),
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			tx := tc.tx
			res, err := tx.Exec(context.Background(), tc.query)
			assert.Nil(t, err)
			lastInsertId, err := res.LastInsertId()
			assert.Nil(t, err)
			assert.EqualValues(t, tc.lastInsertId, lastInsertId)
			rowsAffected, err := res.RowsAffected()
			assert.Nil(t, err)
			if tc.isCommit {
				assert.Nil(t, tx.Commit())
			} else {
				assert.Nil(t, tx.Rollback())
			}
			assert.EqualValues(t, tc.rowsAffected, rowsAffected)
		})
	}
}

func TestSingleSuite(t *testing.T) {
	suite.Run(t, &TransactionSuite{})
}

type mockDB struct {
	db *sql.DB
}

func (m *mockDB) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	return m.db.QueryContext(ctx, query.SQL, query.Args...)
}

func (m *mockDB) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	return m.db.ExecContext(ctx, query.SQL, query.Args...)
}

func (m *mockDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (datasource.Tx, error) {
	tx, err := m.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return transaction.NewTx(tx, m), nil
}

func (m *mockDB) Close() error {
	return m.db.Close()
}

func NewMockDB(db *sql.DB) datasource.DataSource {
	return &mockDB{
		db: db,
	}
}
