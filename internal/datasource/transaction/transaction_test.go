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

package transaction

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ecodeclub/eorm/internal/query"

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
	ds     datasource.DataSource
}

func (db *testMockDB) Query(_ context.Context, _ query.Query) (*sql.Rows, error) {
	return &sql.Rows{}, nil
}

func (db *testMockDB) Exec(_ context.Context, _ query.Query) (sql.Result, error) {
	return nil, nil
}

func openMockDB(driver string, db *sql.DB) *testMockDB {
	return &testMockDB{driver: driver, db: db}
}

func (db *testMockDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return OpenTx(tx, db), nil
}

func (db *testMockDB) Close() error {
	return db.db.Close()
}
