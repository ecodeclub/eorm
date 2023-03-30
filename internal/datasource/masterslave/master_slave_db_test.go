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

package masterslave

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves/roundrobin"

	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"

	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/suite"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func ExampleMasterSlavesDB_Close() {
	masterDB, _ := sql.Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	slaveDB1, _ := sql.Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	slaveDB2, _ := sql.Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	sl, _ := roundrobin.NewSlaves(slaveDB1, slaveDB2)
	ms := NewMasterSlavesDB(masterDB, MasterSlavesWithSlaves(sl))
	err := ms.Close()
	if err == nil {
		fmt.Println("close")
	}

	// Output:
	// close
}

func TestMasterSlavesDB_BeginTx(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()

	db := NewMasterSlavesDB(mockDB)

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
	db := NewMasterSlavesDB(sqlite3db)
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

type MasterSlaveSuite struct {
	suite.Suite
	mockMasterDB *sql.DB
	mockMaster   sqlmock.Sqlmock
	mockSlave1DB *sql.DB
	mockSlave1   sqlmock.Sqlmock
	mockSlave2DB *sql.DB
	mockSlave2   sqlmock.Sqlmock
	mockSlave3DB *sql.DB
	mockSlave3   sqlmock.Sqlmock
}

func (ms *MasterSlaveSuite) SetupTest() {
	t := ms.T()
	ms.initMock(t)
}

func (ms *MasterSlaveSuite) TearDownTest() {
	_ = ms.mockMasterDB.Close()
	_ = ms.mockSlave1DB.Close()
	_ = ms.mockSlave2DB.Close()
	_ = ms.mockSlave3DB.Close()
}

func (ms *MasterSlaveSuite) initMock(t *testing.T) {
	var err error
	ms.mockMasterDB, ms.mockMaster, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	ms.mockSlave1DB, ms.mockSlave1, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	ms.mockSlave2DB, ms.mockSlave2, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	ms.mockSlave3DB, ms.mockSlave3, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
}

func (ms *MasterSlaveSuite) TestMasterSlaveDbQuery() {
	// 通过select不同的数据表示访问不同的db
	ms.mockMaster.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("master"))
	ms.mockSlave1.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("slave1_1"))
	ms.mockSlave2.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("slave1_2"))
	ms.mockSlave3.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("slave1_3"))

	testCasesQuery := []struct {
		name     string
		ctx      context.Context
		query    datasource.Query
		reqCnt   int
		slaves   slaves.Slaves
		wantResp []string
		wantErr  error
	}{
		{
			name:   "select default use slave",
			ctx:    context.Background(),
			reqCnt: 3,
			query: datasource.Query{
				SQL: "SELECT `first_name` FROM `test_model`",
			},
			slaves:   ms.newSlaves(ms.mockSlave1DB, ms.mockSlave2DB, ms.mockSlave3DB),
			wantResp: []string{"slave1_1", "slave1_2", "slave1_3"},
		},
		{
			name:   "use master",
			reqCnt: 1,
			ctx:    UseMaster(context.Background()),
			query: datasource.Query{
				SQL: "SELECT `first_name` FROM `test_model`",
			},
			slaves:   ms.newSlaves(ms.mockSlave1DB, ms.mockSlave2DB, ms.mockSlave3DB),
			wantResp: []string{"master"},
		},
	}

	for _, tc := range testCasesQuery {
		ms.T().Run(tc.name, func(t *testing.T) {
			db := NewMasterSlavesDB(ms.mockMasterDB, MasterSlavesWithSlaves(tc.slaves))
			//  TODO
			//db, ok := source.(*masterSlavesDB)
			//assert.True(t, ok)
			var resp []string
			for i := 1; i <= tc.reqCnt; i++ {
				rows, queryErr := db.Query(tc.ctx, tc.query)
				assert.Equal(t, queryErr, tc.wantErr)
				if queryErr != nil {
					return
				}
				assert.NotNil(t, rows)
				ok := rows.Next()
				assert.True(t, ok)

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

func (ms *MasterSlaveSuite) TestMasterSlaveDbExec() {
	// 使用 sql.Result.LastInsertId 表示请求的是 master或者slave
	ms.mockSlave1.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(2, 1))
	ms.mockSlave2.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(3, 1))
	ms.mockSlave3.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(4, 1))

	testCasesExec := []struct {
		name              string
		ctx               context.Context
		query             datasource.Query
		reqCnt            int
		slaves            slaves.Slaves
		wantRowsAffected  []int64
		wantLastInsertIds []int64
		wantErr           error
	}{
		{
			name:   "null slave",
			ctx:    context.Background(),
			reqCnt: 1,
			query: datasource.Query{
				SQL:  "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(?,?,?,?)",
				Args: []any{1, 2, 3, 4},
			},
			wantRowsAffected:  []int64{1}, // 切片元素表示的是 lastInsertID, 这里表示请求 master db 1 次
			wantLastInsertIds: []int64{1},
		},
		{
			name:   "3 salves",
			ctx:    context.Background(),
			reqCnt: 3,
			query: datasource.Query{
				SQL:  "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(?,?,?,?)",
				Args: []any{1, 2, 3, 4},
			},
			slaves:            ms.newSlaves(ms.mockSlave1DB, ms.mockSlave2DB, ms.mockSlave3DB),
			wantRowsAffected:  []int64{1, 1, 1}, // 切片元素表示的是 lastInsertID, 这里表示请求 master db 3 次
			wantLastInsertIds: []int64{1, 1, 1},
		},
		{
			name:   "use master with 3 slaves",
			reqCnt: 1,
			ctx:    UseMaster(context.Background()),
			query: datasource.Query{
				SQL:  "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(?,?,?,?)",
				Args: []any{1, 2, 3, 4},
			},
			slaves:            ms.newSlaves(ms.mockSlave1DB, ms.mockSlave2DB, ms.mockSlave3DB),
			wantRowsAffected:  []int64{1}, // 切片元素表示的是 lastInsertID, 这里表示请求 master db 1 次
			wantLastInsertIds: []int64{1},
		},
	}

	for _, tc := range testCasesExec {
		ms.T().Run(tc.name, func(t *testing.T) {
			db := NewMasterSlavesDB(ms.mockMasterDB, MasterSlavesWithSlaves(tc.slaves))
			//  TODO
			//db, ok := source.(*masterSlavesDB)
			//assert.True(t, ok)
			var resAffectID []int64
			var resLastID []int64
			for i := 1; i <= tc.reqCnt; i++ {
				ms.mockMaster.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(1, 1))
				res, err := db.Exec(tc.ctx, tc.query)
				assert.Nil(t, err)
				afID, er := res.RowsAffected()
				if er != nil {
					continue
				}
				lastID, er := res.LastInsertId()
				if er != nil {
					continue
				}
				resAffectID = append(resAffectID, afID)
				resLastID = append(resLastID, lastID)
			}
			assert.ElementsMatch(t, tc.wantRowsAffected, resAffectID)
			assert.ElementsMatch(t, tc.wantLastInsertIds, resLastID)

		})
	}
}

func (ms *MasterSlaveSuite) newSlaves(dbs ...*sql.DB) slaves.Slaves {
	res, err := roundrobin.NewSlaves(dbs...)
	require.NoError(ms.T(), err)
	return res
}

func TestMasterSlave(t *testing.T) {
	suite.Run(t, &MasterSlaveSuite{})
}
