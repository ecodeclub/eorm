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
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestDeleter_Build(t *testing.T) {
	testCases := []CommonTestCase{
		{
			name:    "no where",
			builder: NewDeleter[TestModel](memoryDB()).From(&TestModel{}),
			wantSql: "DELETE FROM `test_model`;",
		},
		{
			name:     "where",
			builder:  NewDeleter[TestModel](memoryDB()).Where(C("Id").EQ(16)),
			wantSql:  "DELETE FROM `test_model` WHERE `id`=?;",
			wantArgs: []interface{}{16},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			query, err := c.builder.Build()
			assert.Equal(t, c.wantErr, err)
			assert.Equal(t, c.wantSql, query.SQL)
			assert.Equal(t, c.wantArgs, query.Args)
		})
	}
}

func TestDeleter_Exec(t *testing.T) {

	testCases := []struct {
		name      string
		mockOrder func(mock sqlmock.Sqlmock)
		delete    func(*DB, *testing.T) (sql.Result, error)
		wantErr   error
		wantVal   sql.Result
	}{
		{
			name: "直接删除",
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `test_model` WHERE `id`=").WithArgs(1).WillReturnResult(sqlmock.NewResult(100, 1000))
			},
			delete: func(db *DB, t *testing.T) (sql.Result, error) {
				deleter := NewDeleter[TestModel](db)
				result, err := deleter.From(&TestModel{}).Where(C("Id").EQ(1)).Exec(context.Background())
				return result, err
			},
			wantErr: nil,
			wantVal: sqlmock.NewResult(100, 1000),
		},
		{
			name: "事务删除",
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("DELETE FROM `test_model` WHERE `id`=").WithArgs(1).WillReturnResult(sqlmock.NewResult(10, 20))
				mock.ExpectCommit().WillReturnError(errors.New("commit 错误"))
			},
			delete: func(db *DB, t *testing.T) (sql.Result, error) {
				tx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
				require.NoError(t, err)

				deleter := NewDeleter[TestModel](db)
				result, err := deleter.From(&TestModel{}).Where(C("Id").EQ(1)).Exec(context.Background())
				require.NoError(t, err)

				err = tx.Commit()
				return result, err
			},
			wantErr: errors.New("commit 错误"),
			wantVal: sqlmock.NewResult(10, 20),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			db, err := openDB("mysql", mockDB)
			defer func(db *DB) { _ = db.Close() }(db)
			if err != nil {
				t.Fatal(err)
			}
			tc.mockOrder(mock)
			result, err := tc.delete(db, t)

			assert.Equal(t, tc.wantErr, err)
			assert.Same(t, reflect.ValueOf(tc.wantVal), reflect.ValueOf(result).Field(1).Elem())

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func ExampleDeleter_Build() {
	query, _ := NewDeleter[TestModel](memoryDB()).From(&TestModel{}).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: DELETE FROM `test_model`;
}

func ExampleDeleter_From() {
	query, _ := NewDeleter[TestModel](memoryDB()).From(&TestModel{}).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: DELETE FROM `test_model`;
}

func ExampleDeleter_Where() {
	query, _ := NewDeleter[TestModel](memoryDB()).Where(C("Id").EQ(12)).Build()
	fmt.Printf("SQL: %s\nArgs: %v", query.SQL, query.Args)
	// Output:
	// SQL: DELETE FROM `test_model` WHERE `id`=?;
	// Args: [12]
}
