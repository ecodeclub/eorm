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
	"github.com/gotomicro/eorm/internal/errs"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gotomicro/eorm/internal/slaves/roundrobin"
	"github.com/stretchr/testify/assert"
)

func TestMasterSlavesDB_BeginTx(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()

	db, err := OpenMasterSlaveDB("mysql", mockDB)
	if err != nil {
		t.Fatal(err)
	}
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

func ExampleMasterSlavesDB_BeginTx() {
	sqlite3db, _ := sql.Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	db, _ := OpenMasterSlaveDB("sqlite3", sqlite3db)
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
	if err == nil {
		fmt.Println("Begin")
	}
	err = tx.Commit()
	if err == nil {
		fmt.Println("Commit")
	}
	// Output:
	// Begin
	// Commit
}

var (
	mockMasterDB *sql.DB
	mockMaster   sqlmock.Sqlmock
	mockSlave1DB *sql.DB
	mockSlave1   sqlmock.Sqlmock
	mockSlave2DB *sql.DB
	mockSlave2   sqlmock.Sqlmock
	mockSlave3DB *sql.DB
	mockSlave3   sqlmock.Sqlmock
)

func initMock(t *testing.T) {
	var err error
	mockMasterDB, mockMaster, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mockSlave1DB, mockSlave1, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mockSlave2DB, mockSlave2, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mockSlave3DB, mockSlave3, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

}

func Test_MasterSlaveDb_Query(t *testing.T) {
	defer func() {
		_ = mockMasterDB.Close()
		_ = mockSlave1DB.Close()
		_ = mockSlave2DB.Close()
		_ = mockSlave3DB.Close()
	}()

	initMock(t)

	mockMaster.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("master"))
	mockSlave1.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("slave1_1"))
	mockSlave2.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("slave1_2"))
	mockSlave3.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("slave1_3"))

	testCasesQuery := []struct {
		name      string
		ctx       context.Context
		query     string
		reqCnt    int
		slaves    *roundrobin.Slaves
		wantReqDb []string
		wantErr   error
	}{
		{
			name:    "select null slave",
			ctx:     context.Background(),
			reqCnt:  1,
			query:   "SELECT `first_name` FROM `test_model`",
			wantErr: errs.ErrSlaveNotFound,
		},
		{
			name:      "select default use slave",
			ctx:       context.Background(),
			reqCnt:    3,
			query:     "SELECT `first_name` FROM `test_model`",
			slaves:    roundrobin.NewSlaves(mockSlave1DB, mockSlave2DB, mockSlave3DB),
			wantReqDb: []string{"slave1_1", "slave1_2", "slave1_3"},
		},
		{
			name:      "use master",
			reqCnt:    1,
			ctx:       UseMaster(context.Background()),
			query:     "SELECT `first_name` FROM `test_model`",
			slaves:    roundrobin.NewSlaves(mockSlave1DB, mockSlave2DB, mockSlave3DB),
			wantReqDb: []string{"master"},
		},
	}

	for _, tc := range testCasesQuery {
		t.Run(tc.name, func(t *testing.T) {
			db, openErr := OpenMasterSlaveDB("mysql", mockMasterDB, MasterSlaveWithSlaves(tc.slaves))
			assert.Nil(t, openErr)

			queryer := RawQuery[string](db, tc.query)
			var resDb []string
			for i := 1; i <= tc.reqCnt; i++ {
				res, queryErr := queryer.Get(tc.ctx)
				assert.Equal(t, queryErr, tc.wantErr)
				if queryErr != nil {
					return
				}
				assert.NotNil(t, res)
				resDb = append(resDb, *res)
			}
			assert.ElementsMatch(t, tc.wantReqDb, resDb)
		})
	}
}

func Test_MasterSlaveDb_Exec(t *testing.T) {
	initMock(t)
	defer func() {
		_ = mockMasterDB.Close()
		_ = mockSlave1DB.Close()
		_ = mockSlave2DB.Close()
		_ = mockSlave3DB.Close()
	}()

	// use sql.Result.LastInsertId to represent master/slave
	mockSlave1.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(2, 1))
	mockSlave2.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(3, 1))
	mockSlave3.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(4, 1))

	testCasesExec := []struct {
		name      string
		ctx       context.Context
		insertSQL string
		reqCnt    int
		slaves    *roundrobin.Slaves
		wantReqDb []int64
		wantErr   error
	}{
		{
			name:      "null slave",
			ctx:       context.Background(),
			reqCnt:    1,
			insertSQL: "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4)",
			wantReqDb: []int64{1}, // match lastInsertID, means request master 1 time
		},
		{
			name:      "3 salves",
			ctx:       context.Background(),
			reqCnt:    3,
			insertSQL: "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4)",
			slaves:    roundrobin.NewSlaves(mockSlave1DB, mockSlave2DB, mockSlave3DB),
			wantReqDb: []int64{1, 1, 1}, // match lastInsertID, means request master 3 time
		},
		{
			name:      "use master with 3 slaves",
			reqCnt:    1,
			ctx:       UseMaster(context.Background()),
			insertSQL: "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4)",
			slaves:    roundrobin.NewSlaves(mockSlave1DB, mockSlave2DB, mockSlave3DB),
			wantReqDb: []int64{1}, // match lastInsertID, means request master 3 time
		},
	}

	for _, tc := range testCasesExec {
		t.Run(tc.name, func(t *testing.T) {
			db, openErr := OpenMasterSlaveDB("mysql", mockMasterDB, MasterSlaveWithSlaves(tc.slaves))
			assert.Nil(t, openErr)
			queryer := RawQuery[string](db, tc.insertSQL)

			var resAffectID []int64
			for i := 1; i <= tc.reqCnt; i++ {
				mockMaster.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(1, 1))
				res := queryer.Exec(tc.ctx)
				if res.Err() != nil {
					continue
				}
				afID, er := res.LastInsertId()
				if er != nil {
					continue
				}
				resAffectID = append(resAffectID, afID)
			}
			assert.ElementsMatch(t, tc.wantReqDb, resAffectID)
		})
	}
}
