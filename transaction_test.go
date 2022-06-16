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
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTx_Commit(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()

	db, err := openDB("mysql", mockDB)
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

	db, err := openDB("mysql", mockDB)
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