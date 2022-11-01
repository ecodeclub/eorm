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
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestResult_res(t *testing.T) {
	tm := &TestModel{
		Id:        12,
		FirstName: "Tom",
		Age:       18,
		LastName:  &sql.NullString{String: "Jerry", Valid: true},
	}
	testCases := []struct {
		name         string
		exec         func(*DB, *testing.T) Result
		mockOrder    func(mock sqlmock.Sqlmock)
		wantErr      error
		wantAffected int64
		wantInsertId int64
	}{
		{
			name: "db server err",
			exec: func(db *DB, t *testing.T) Result {
				result := Result{err: errors.New("服务器异常")}
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("").WillReturnError(errors.New("服务器异常"))
			},
			wantErr: errors.New("服务器异常"),
		},
		{
			name: "update rows",
			exec: func(db *DB, t *testing.T) Result {
				updater := NewUpdater[TestModel](db).Update(tm).Set(Columns("FirstName"))
				result := updater.Exec(context.Background())
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE `test_model` SET `first_name`=").
					WithArgs("Tom").WillReturnResult(sqlmock.NewResult(100, 1))
			},
			wantAffected: int64(1),
			wantInsertId: int64(100),
		},
		{
			name: "delete row",
			exec: func(db *DB, t *testing.T) Result {
				deleter := NewDeleter[TestModel](db)
				result := deleter.From(&TestModel{}).Where(C("Id").EQ(11)).Exec(context.Background())
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `test_model` WHERE `id`=").
					WithArgs(11).WillReturnResult(sqlmock.NewResult(100, 1))
			},
			wantAffected: int64(1),
			wantInsertId: int64(100),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Error(err)
			}
			orm, err := openDB("mysql", mockDB)
			defer func(db *DB) {
				_ = orm.Close()
			}(orm)
			tc.mockOrder(mock)
			res := tc.exec(orm, t)
			assert.Equal(t, tc.wantErr, res.Err())
			if res.Err() != nil {
				return
			}
			assert.Nil(t, tc.wantErr)
			rowsAffected, err := res.RowsAffected()
			require.NoError(t, err)

			lastInsertId, err := res.LastInsertId()
			require.NoError(t, err)

			assert.Equal(t, tc.wantInsertId, lastInsertId)
			assert.Equal(t, tc.wantAffected, rowsAffected)

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}
