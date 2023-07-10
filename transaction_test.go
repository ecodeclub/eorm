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

package eorm

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/single"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTx_Commit(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()

	db, err := OpenDS("mysql", single.NewDB(mockDB))
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

	db, err := OpenDS("mysql", single.NewDB(mockDB))
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

func TestTx_QueryContext(t *testing.T) {
	testCases := []struct {
		name       string
		query      Query
		mockOrder  func(mock sqlmock.Sqlmock)
		sourceFunc func(db *sql.DB, t *testing.T) datasource.DataSource
		wantResp   []string
		wantErr    error
		isCommit   bool
	}{
		{
			name: "err",
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery("SELECT `xx` FROM `test_model`").
					WillReturnError(errors.New("未知字段"))
				mock.ExpectRollback()
			},
			sourceFunc: func(db *sql.DB, t *testing.T) datasource.DataSource {
				return single.NewDB(db)
			},
			query: Query{
				SQL: "SELECT `xx` FROM `test_model`",
			},
			wantErr:  errors.New("未知字段"),
			isCommit: false,
		},
		{
			name: "commit",
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery("SELECT `first_name` FROM `test_model`").
					WillReturnRows(sqlmock.NewRows([]string{"first_name"}).AddRow("value"))
				mock.ExpectCommit()
			},
			sourceFunc: func(db *sql.DB, t *testing.T) datasource.DataSource {
				return single.NewDB(db)
			},
			query: Query{
				SQL: "SELECT `first_name` FROM `test_model`",
			},
			isCommit: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func(db *sql.DB) { _ = db.Close() }(mockDB)
			tc.mockOrder(mock)
			source := tc.sourceFunc(mockDB, t)
			orm, err := OpenDS("mysql", source)
			require.NoError(t, err)
			tx, err := orm.BeginTx(context.Background(), &sql.TxOptions{})
			require.NoError(t, err)
			rows, queryErr := tx.queryContext(context.Background(), datasource.Query(tc.query))
			assert.Equal(t, queryErr, tc.wantErr)
			if queryErr != nil {
				return
			}

			if tc.isCommit {
				err = tx.Commit()
			} else {
				err = tx.Rollback()
			}
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
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
			if err = mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestTx_ExecContext(t *testing.T) {
	testCases := []struct {
		name           string
		query          Query
		mockOrder      func(mock sqlmock.Sqlmock)
		sourceFunc     func(db *sql.DB, t *testing.T) datasource.DataSource
		wantVal        sql.Result
		wantBeginTxErr error
		wantErr        error
		isCommit       bool
	}{
		//{
		//	name: "source err",
		//	mockOrder: func(mock sqlmock.Sqlmock) {
		//		mock.ExpectBegin()
		//		mock.ExpectExec("DELETE FROM `test_model` WHERE `id`=").WithArgs(1).WillReturnResult(sqlmock.NewResult(10, 20))
		//		mock.ExpectCommit()
		//	},
		//	sourceFunc: func(db *sql.DB, t *testing.T) datasource.DataSource {
		//		clusterDB := cluster.NewClusterDB(map[string]*masterslave.MasterSlavesDB{
		//			"db0": masterslave.NewMasterSlavesDB(db),
		//		})
		//		return clusterDB
		//	},
		//	query: Query{
		//		SQL:  "DELETE FROM `test_model` WHERE `id`=",
		//		Args: []any{1},
		//	},
		//	wantBeginTxErr: errors.New("eorm: 未实现 TxBeginner 接口"),
		//},
		{
			name: "commit err",
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("DELETE FROM `test_model` WHERE `id`=").WithArgs(1).WillReturnResult(sqlmock.NewResult(10, 20))
				mock.ExpectCommit().WillReturnError(errors.New("commit 错误"))
			},
			sourceFunc: func(db *sql.DB, t *testing.T) datasource.DataSource {
				return single.NewDB(db)
			},
			query: Query{
				SQL:  "DELETE FROM `test_model` WHERE `id`=",
				Args: []any{1},
			},
			wantErr:  errors.New("commit 错误"),
			isCommit: true,
		},
		{
			name: "rollback err",
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("DELETE FROM `test_model` WHERE `id`=").WithArgs(1).WillReturnResult(sqlmock.NewResult(10, 20))
				mock.ExpectRollback().WillReturnError(errors.New("rollback 错误"))
			},
			sourceFunc: func(db *sql.DB, t *testing.T) datasource.DataSource {
				return single.NewDB(db)
			},
			query: Query{
				SQL:  "DELETE FROM `test_model` WHERE `id`=",
				Args: []any{1},
			},
			wantErr: errors.New("rollback 错误"),
		},
		{
			name: "commit",
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("DELETE FROM `test_model` WHERE `id`=").WithArgs(1).WillReturnResult(sqlmock.NewResult(10, 20))
				mock.ExpectCommit()
			},
			sourceFunc: func(db *sql.DB, t *testing.T) datasource.DataSource {
				return single.NewDB(db)
			},
			query: Query{
				SQL:  "DELETE FROM `test_model` WHERE `id`=",
				Args: []any{1},
			},
			wantVal:  sqlmock.NewResult(10, 20),
			isCommit: true,
		},
		{
			name: "rollback",
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("DELETE FROM `test_model` WHERE `id`=").WithArgs(1).WillReturnResult(sqlmock.NewResult(10, 20))
				mock.ExpectRollback()
			},
			sourceFunc: func(db *sql.DB, t *testing.T) datasource.DataSource {
				return single.NewDB(db)
			},
			query: Query{
				SQL:  "DELETE FROM `test_model` WHERE `id`=",
				Args: []any{1},
			},
			wantVal: sqlmock.NewResult(10, 20),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func(db *sql.DB) { _ = db.Close() }(mockDB)
			tc.mockOrder(mock)

			source := tc.sourceFunc(mockDB, t)
			orm, err := OpenDS("mysql", source)
			require.NoError(t, err)
			tx, err := orm.BeginTx(context.Background(), &sql.TxOptions{})
			assert.Equal(t, tc.wantBeginTxErr, err)
			if err != nil {
				return
			}
			result, err := tx.execContext(context.Background(), datasource.Query(tc.query))
			require.NoError(t, err)

			if tc.isCommit {
				err = tx.Commit()
			} else {
				err = tx.Rollback()
			}
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}

			rowsAffectedExpect, err := tc.wantVal.RowsAffected()
			require.NoError(t, err)
			rowsAffected, err := result.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, rowsAffectedExpect, rowsAffected)

			lastInsertIdExpected, err := tc.wantVal.LastInsertId()
			require.NoError(t, err)
			lastInsertId, err := result.LastInsertId()
			require.NoError(t, err)
			assert.Equal(t, lastInsertIdExpected, lastInsertId)

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}
