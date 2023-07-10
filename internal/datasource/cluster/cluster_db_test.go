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

package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves/roundrobin"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	slaves2 "github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"

	"github.com/ecodeclub/eorm/internal/errs"
	_ "github.com/mattn/go-sqlite3"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/sharding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func Example_clusterDB_Close() {
	db, _ := sql.Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	cl := NewClusterDB(map[string]*masterslave.MasterSlavesDB{
		"db0": masterslave.NewMasterSlavesDB(db),
	})
	err := cl.Close()
	if err == nil {
		fmt.Println("close")
	}

	// Output:
	// close
}

type ClusterSuite struct {
	suite.Suite
	datasource.DataSource
	mockMasterDB *sql.DB
	mockMaster   sqlmock.Sqlmock

	mockSlave1DB *sql.DB
	mockSlave1   sqlmock.Sqlmock

	mockSlave2DB *sql.DB
	mockSlave2   sqlmock.Sqlmock

	mockSlave3DB *sql.DB
	mockSlave3   sqlmock.Sqlmock
}

func (c *ClusterSuite) SetupTest() {
	t := c.T()
	c.initMock(t)
}

func (c *ClusterSuite) TearDownTest() {
	_ = c.mockMasterDB.Close()
	_ = c.mockSlave1DB.Close()
	_ = c.mockSlave2DB.Close()
	_ = c.mockSlave3DB.Close()
}

func (c *ClusterSuite) initMock(t *testing.T) {
	var err error
	c.mockMasterDB, c.mockMaster, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	c.mockSlave1DB, c.mockSlave1, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	c.mockSlave2DB, c.mockSlave2, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	c.mockSlave3DB, c.mockSlave3, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
}

func (c *ClusterSuite) TestClusterDbQuery() {
	// 通过select不同的数据表示访问不同的db
	c.mockMaster.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("master"))
	c.mockSlave1.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("slave1_1"))
	c.mockSlave2.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("slave1_2"))
	c.mockSlave3.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("slave1_3"))

	db := masterslave.NewMasterSlavesDB(c.mockMasterDB, masterslave.MasterSlavesWithSlaves(
		c.newSlaves(c.mockSlave1DB, c.mockSlave2DB, c.mockSlave3DB)))
	testCasesQuery := []struct {
		name     string
		reqCnt   int
		ctx      context.Context
		query    sharding.Query
		ms       map[string]*masterslave.MasterSlavesDB
		wantResp []string
		wantErr  error
	}{
		{
			name:   "query not found target db",
			ctx:    context.Background(),
			reqCnt: 3,
			query: sharding.Query{
				SQL: "SELECT `first_name` FROM `test_model`",
				DB:  "order_db_1",
			},
			ms: func() map[string]*masterslave.MasterSlavesDB {
				masterSlaves := map[string]*masterslave.MasterSlavesDB{"order_db_0": db}
				return masterSlaves
			}(),
			wantErr: errs.NewErrNotFoundTargetDB("order_db_1"),
		},
		{
			name:   "select default use slave",
			ctx:    context.Background(),
			reqCnt: 3,
			query: sharding.Query{
				SQL: "SELECT `first_name` FROM `test_model`",
				DB:  "order_db_0",
			},
			ms: func() map[string]*masterslave.MasterSlavesDB {
				masterSlaves := map[string]*masterslave.MasterSlavesDB{"order_db_0": db}
				return masterSlaves
			}(),
			wantResp: []string{"slave1_1", "slave1_2", "slave1_3"},
		},
		{
			name:   "use master",
			reqCnt: 1,
			ctx:    masterslave.UseMaster(context.Background()),
			query: sharding.Query{
				SQL: "SELECT `first_name` FROM `test_model`",
				DB:  "order_db_1",
			},
			ms: func() map[string]*masterslave.MasterSlavesDB {
				masterSlaves := map[string]*masterslave.MasterSlavesDB{"order_db_1": db}
				return masterSlaves
			}(),
			wantResp: []string{"master"},
		},
	}

	for _, tc := range testCasesQuery {
		c.T().Run(tc.name, func(t *testing.T) {
			clusDB := NewClusterDB(tc.ms)
			var resp []string
			for i := 1; i <= tc.reqCnt; i++ {
				rows, queryErr := clusDB.Query(tc.ctx, tc.query)
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

func (c *ClusterSuite) TestClusterDbExec() {
	// 使用 sql.Result.LastInsertId 表示请求的是 master或者slave
	c.mockSlave1.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(2, 1))
	c.mockSlave2.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(3, 1))
	c.mockSlave3.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(4, 1))

	testCasesExec := []struct {
		name     string
		reqCnt   int
		ctx      context.Context
		slaves   slaves2.Slaves
		query    sharding.Query
		ms       map[string]*masterslave.MasterSlavesDB
		wantResp []int64
		wantErr  error
	}{
		{
			name:   "exec not found target db",
			ctx:    context.Background(),
			reqCnt: 1,
			query: sharding.Query{
				SQL: "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4)",
				DB:  "order_db_1",
			},
			ms: func() map[string]*masterslave.MasterSlavesDB {
				db := masterslave.NewMasterSlavesDB(c.mockMasterDB,
					masterslave.MasterSlavesWithSlaves(c.newSlaves(nil)))
				masterSlaves := map[string]*masterslave.MasterSlavesDB{"order_db_0": db}
				return masterSlaves
			}(),
			wantErr: errs.NewErrNotFoundTargetDB("order_db_1"),
		},
		{
			name:   "null slave",
			ctx:    context.Background(),
			reqCnt: 1,
			query: sharding.Query{
				SQL: "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4)",
				DB:  "order_db_0",
			},
			ms: func() map[string]*masterslave.MasterSlavesDB {
				db := masterslave.NewMasterSlavesDB(c.mockMasterDB,
					masterslave.MasterSlavesWithSlaves(c.newSlaves(nil)))
				masterSlaves := map[string]*masterslave.MasterSlavesDB{"order_db_0": db}
				return masterSlaves
			}(),
			wantResp: []int64{1}, // 切片元素表示的是 lastInsertID, 这里表示请求 master db 1 次
		},
		{
			name:   "3 salves",
			ctx:    context.Background(),
			reqCnt: 3,
			query: sharding.Query{
				SQL: "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4)",
				DB:  "order_db_1",
			},
			ms: func() map[string]*masterslave.MasterSlavesDB {
				db := masterslave.NewMasterSlavesDB(c.mockMasterDB, masterslave.MasterSlavesWithSlaves(
					c.newSlaves(c.mockSlave1DB, c.mockSlave2DB, c.mockSlave3DB)))
				masterSlaves := map[string]*masterslave.MasterSlavesDB{"order_db_1": db}
				return masterSlaves
			}(),
			wantResp: []int64{1, 1, 1}, // 切片元素表示的是 lastInsertID, 这里表示请求 master db 3 次
		},
		{
			name:   "use master with 3 slaves",
			reqCnt: 1,
			ctx:    masterslave.UseMaster(context.Background()),
			query: sharding.Query{
				SQL: "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4)",
				DB:  "order_db_2",
			},
			ms: func() map[string]*masterslave.MasterSlavesDB {
				db := masterslave.NewMasterSlavesDB(c.mockMasterDB, masterslave.MasterSlavesWithSlaves(
					c.newSlaves(c.mockSlave1DB, c.mockSlave2DB, c.mockSlave3DB)))
				masterSlaves := map[string]*masterslave.MasterSlavesDB{"order_db_2": db}
				return masterSlaves
			}(),
			wantResp: []int64{1}, // 切片元素表示的是 lastInsertID, 这里表示请求 master db 1 次
		},
	}

	for _, tc := range testCasesExec {
		c.T().Run(tc.name, func(t *testing.T) {
			db := NewClusterDB(tc.ms)
			var resAffectID []int64
			for i := 1; i <= tc.reqCnt; i++ {
				c.mockMaster.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(1, 1))
				res, execErr := db.Exec(tc.ctx, tc.query)
				assert.Equal(t, execErr, tc.wantErr)
				if execErr != nil {
					return
				}

				afID, er := res.LastInsertId()
				if er != nil {
					continue
				}
				resAffectID = append(resAffectID, afID)
			}
			assert.ElementsMatch(t, tc.wantResp, resAffectID)
		})
	}
}

func (c *ClusterSuite) newSlaves(dbs ...*sql.DB) slaves2.Slaves {
	res, err := roundrobin.NewSlaves(dbs...)
	require.NoError(c.T(), err)
	return res
}

func TestClusterDB(t *testing.T) {
	suite.Run(t, &ClusterSuite{})
}
