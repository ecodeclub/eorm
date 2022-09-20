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
	"github.com/DATA-DOG/go-sqlmock"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleter_Build(t *testing.T) {
	testCases := []CommonTestCase{
		{
			name:    "no where",
			builder: memoryDB().Delete().From(&TestModel{}),
			wantSql: "DELETE FROM `test_model`;",
		},
		{
			name:     "where",
			builder:  memoryDB().Delete().From(&TestModel{}).Where(C("Id").EQ(16)),
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
		dbOrder   func(*DB) ([]sql.Result, []error)
		wantErrs  []error
		wantVals  []sql.Result
	}{
		{
			name: "直接删除",
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `product` WHERE `id`=").WithArgs(1).WillReturnResult(sqlmock.NewResult(100, 1000))
			},
			dbOrder: func(db *DB) ([]sql.Result, []error) {
				results := make([]sql.Result, 0, 0)
				errs := make([]error, 0, 0)

				exec, err := db.Delete().From(&Product{}).Where(C("Id").EQ(1)).Exec(context.Background())
				results = append(results, exec)
				errs = append(errs, err)

				return results, errs
			},
			wantErrs: []error{nil},
			wantVals: []sql.Result{sqlmock.NewResult(100, 1000)},
		},
		{
			name: "事务删除",
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("DELETE FROM `product` WHERE `id`=").WithArgs(1).WillReturnResult(sqlmock.NewResult(10, 20))
				mock.ExpectCommit().WillReturnError(errors.New("最后一步"))
			},
			dbOrder: func(db *DB) ([]sql.Result, []error) {
				results := make([]sql.Result, 0, 0)
				errs := make([]error, 0, 0)

				tx, _ := db.BeginTx(context.Background(), &sql.TxOptions{})
				exec, err := db.Delete().From(&Product{}).Where(C("Id").EQ(1)).ExecWithTx(context.Background(), tx)
				results = append(results, exec)
				errs = append(errs, err)
				err = tx.Commit()
				errs = append(errs, err)

				return results, errs
			},
			wantErrs: []error{nil, errors.New("最后一步")},
			wantVals: []sql.Result{sqlmock.NewResult(10, 20)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			db, err := openDB("mysql", mockDB)
			defer func(db *DB) { _ = db.Close() }(db)
			if err != nil {
				t.Fatal(err)
			}
			tc.mockOrder(mock)
			results, errs := tc.dbOrder(db)

			assert.Equal(t, len(tc.wantVals), len(results))
			assert.Equal(t, len(tc.wantErrs), len(errs))

			for i, result := range results {
				wantLastId, _ := tc.wantVals[i].LastInsertId()
				lastId, _ := result.LastInsertId()
				assert.Equal(t, wantLastId, lastId)

				wantAffected, _ := tc.wantVals[i].RowsAffected()
				affected, _ := result.RowsAffected()
				assert.Equal(t, wantAffected, affected)
			}

			for i, errR := range errs {
				assert.Equal(t, tc.wantErrs[i], errR)
			}
			if err = mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func ExampleDeleter_Build() {
	query, _ := memoryDB().Delete().From(&TestModel{}).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: DELETE FROM `test_model`;
}

func ExampleDeleter_From() {
	query, _ := memoryDB().Delete().From(&TestModel{}).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: DELETE FROM `test_model`;
}

func ExampleDeleter_Where() {
	query, _ := memoryDB().Delete().From(&TestModel{}).Where(C("Id").EQ(12)).Build()
	fmt.Printf("SQL: %s\nArgs: %v", query.SQL, query.Args)
	// Output:
	// SQL: DELETE FROM `test_model` WHERE `id`=?;
	// Args: [12]
}

type Product struct {
	Id   int64
	Sale float64
	Size int64
}
