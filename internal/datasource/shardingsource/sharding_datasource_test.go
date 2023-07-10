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

package shardingsource

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves/roundrobin"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"

	"github.com/ecodeclub/eorm/internal/errs"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/cluster"
	"github.com/ecodeclub/eorm/internal/sharding"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func ExampleShardingDataSource_Close() {
	db, _ := sql.Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	cl := cluster.NewClusterDB(map[string]*masterslave.MasterSlavesDB{
		"db0": masterslave.NewMasterSlavesDB(db),
	})
	ds := NewShardingDataSource(map[string]datasource.DataSource{
		"source0": cl,
	})
	err := ds.Close()
	if err == nil {
		fmt.Println("close")
	}

	// Output:
	// close
}

type ShardingDataSourceSuite struct {
	suite.Suite
	datasource.DataSource
	mockMaster1DB *sql.DB
	mockMaster    sqlmock.Sqlmock

	mockSlave1DB *sql.DB
	mockSlave1   sqlmock.Sqlmock

	mockSlave2DB *sql.DB
	mockSlave2   sqlmock.Sqlmock

	mockSlave3DB *sql.DB
	mockSlave3   sqlmock.Sqlmock

	mockMaster2DB *sql.DB
	mockMaster2   sqlmock.Sqlmock

	mockSlave4DB *sql.DB
	mockSlave4   sqlmock.Sqlmock

	mockSlave5DB *sql.DB
	mockSlave5   sqlmock.Sqlmock

	mockSlave6DB *sql.DB
	mockSlave6   sqlmock.Sqlmock
}

func (c *ShardingDataSourceSuite) SetupTest() {
	t := c.T()
	c.initMock(t)
}

func (c *ShardingDataSourceSuite) TearDownTest() {
	_ = c.mockMaster1DB.Close()
	_ = c.mockSlave1DB.Close()
	_ = c.mockSlave2DB.Close()
	_ = c.mockSlave3DB.Close()

	_ = c.mockMaster2DB.Close()
	_ = c.mockSlave4DB.Close()
	_ = c.mockSlave5DB.Close()
	_ = c.mockSlave6DB.Close()
}

func (c *ShardingDataSourceSuite) initMock(t *testing.T) {
	var err error
	c.mockMaster1DB, c.mockMaster, err = sqlmock.New()
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

	c.mockMaster2DB, c.mockMaster2, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	c.mockSlave4DB, c.mockSlave4, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	c.mockSlave5DB, c.mockSlave5, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	c.mockSlave6DB, c.mockSlave6, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	db1 := masterslave.NewMasterSlavesDB(c.mockMaster1DB, masterslave.MasterSlavesWithSlaves(
		c.newSlaves(c.mockSlave1DB, c.mockSlave2DB, c.mockSlave3DB)))

	db2 := masterslave.NewMasterSlavesDB(c.mockMaster2DB, masterslave.MasterSlavesWithSlaves(
		c.newSlaves(c.mockSlave4DB, c.mockSlave5DB, c.mockSlave6DB)))

	clusterDB1 := cluster.NewClusterDB(map[string]*masterslave.MasterSlavesDB{"db_0": db1})
	clusterDB2 := cluster.NewClusterDB(map[string]*masterslave.MasterSlavesDB{"db_0": db2})

	c.DataSource = NewShardingDataSource(map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB1,
		"1.db.cluster.company.com:3306": clusterDB2,
	})

}

func (c *ShardingDataSourceSuite) TestClusterDbQuery() {
	// 通过select不同的数据表示访问不同的db
	c.mockMaster.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("cluster0 master"))
	c.mockSlave1.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("cluster0 slave1_1"))
	c.mockSlave2.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("cluster0 slave1_2"))
	c.mockSlave3.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("cluster0 slave1_3"))

	c.mockMaster2.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("cluster1 master"))
	c.mockSlave4.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("cluster1 slave1_1"))
	c.mockSlave5.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("cluster1 slave1_2"))
	c.mockSlave6.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("cluster1 slave1_3"))

	testCasesQuery := []struct {
		name     string
		reqCnt   int
		ctx      context.Context
		query    sharding.Query
		wantResp []string
		wantErr  error
	}{
		{
			name:   "not found target DataSource",
			ctx:    context.Background(),
			reqCnt: 1,
			query: sharding.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Datasource: "2.db.cluster.company.com:3306",
			},
			wantErr: errs.NewErrNotFoundTargetDataSource("2.db.cluster.company.com:3306"),
		},
		{
			name:   "cluster0 select default use slave",
			ctx:    context.Background(),
			reqCnt: 3,
			query: sharding.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Datasource: "0.db.cluster.company.com:3306",
			},
			wantResp: []string{"cluster0 slave1_1", "cluster0 slave1_2", "cluster0 slave1_3"},
		},
		{
			name:   "cluster1 select default use slave",
			ctx:    context.Background(),
			reqCnt: 3,
			query: sharding.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Datasource: "1.db.cluster.company.com:3306",
			},
			wantResp: []string{"cluster1 slave1_1", "cluster1 slave1_2", "cluster1 slave1_3"},
		},
		{
			name:   "cluster0 use master",
			reqCnt: 1,
			ctx:    masterslave.UseMaster(context.Background()),
			query: sharding.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Datasource: "0.db.cluster.company.com:3306",
			},
			wantResp: []string{"cluster0 master"},
		},
		{
			name:   "cluster1 use master",
			reqCnt: 1,
			ctx:    masterslave.UseMaster(context.Background()),
			query: sharding.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Datasource: "1.db.cluster.company.com:3306",
			},
			wantResp: []string{"cluster1 master"},
		},
	}

	for _, tc := range testCasesQuery {
		c.T().Run(tc.name, func(t *testing.T) {
			var resp []string
			for i := 1; i <= tc.reqCnt; i++ {
				rows, queryErr := c.DataSource.Query(tc.ctx, tc.query)
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

func (c *ShardingDataSourceSuite) TestClusterDbExec() {
	// 使用 sql.Result.LastInsertId 表示请求的是 master或者slave
	c.mockMaster.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(1, 1))
	c.mockMaster2.ExpectExec("^INSERT INTO (.+)").WillReturnResult(sqlmock.NewResult(2, 1))

	testCasesExec := []struct {
		name              string
		reqCnt            int
		ctx               context.Context
		slaves            slaves.Slaves
		query             sharding.Query
		wantRowsAffected  []int64
		wantLastInsertIds []int64
		wantErr           error
	}{
		{
			name:   "not found target DataSource",
			ctx:    context.Background(),
			reqCnt: 1,
			query: sharding.Query{
				SQL:        "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4)",
				DB:         "db_0",
				Datasource: "2.db.cluster.company.com:3306",
			},
			wantErr: errs.NewErrNotFoundTargetDataSource("2.db.cluster.company.com:3306"),
		},
		{
			name:   "cluster0 exec",
			reqCnt: 1,
			ctx:    masterslave.UseMaster(context.Background()),
			query: sharding.Query{
				SQL:        "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4)",
				DB:         "db_0",
				Datasource: "0.db.cluster.company.com:3306",
			},
			wantRowsAffected:  []int64{1},
			wantLastInsertIds: []int64{1},
		},
		{
			name:   "cluster1 exec",
			reqCnt: 1,
			ctx:    masterslave.UseMaster(context.Background()),
			query: sharding.Query{
				SQL:        "INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(1,2,3,4)",
				DB:         "db_0",
				Datasource: "1.db.cluster.company.com:3306",
			},
			wantRowsAffected:  []int64{1},
			wantLastInsertIds: []int64{2},
		},
	}

	for _, tc := range testCasesExec {
		c.T().Run(tc.name, func(t *testing.T) {
			var resAffectID []int64
			var resLastID []int64
			for i := 1; i <= tc.reqCnt; i++ {
				res, execErr := c.DataSource.Exec(tc.ctx, tc.query)
				assert.Equal(t, execErr, tc.wantErr)
				if execErr != nil {
					return
				}
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

func (c *ShardingDataSourceSuite) newSlaves(dbs ...*sql.DB) slaves.Slaves {
	res, err := roundrobin.NewSlaves(dbs...)
	require.NoError(c.T(), err)
	return res
}

func TestShardingDataSourceSuite(t *testing.T) {
	suite.Run(t, &ShardingDataSourceSuite{})
}
