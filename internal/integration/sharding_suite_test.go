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

//go:build e2e

package integration

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"time"

	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/cluster"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves/roundrobin"
	"github.com/ecodeclub/eorm/internal/datasource/shardingsource"
	"github.com/ecodeclub/eorm/internal/model"
	"github.com/ecodeclub/eorm/internal/sharding"
	"github.com/ecodeclub/eorm/internal/sharding/hash"
	"github.com/ecodeclub/eorm/internal/test"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ShardingSuite struct {
	suite.Suite
	slaves      slaves.Slaves
	clusters    *clusterDrivers
	shardingDB  *eorm.DB
	algorithm   sharding.Algorithm
	dataSources map[string]datasource.DataSource
	driver      string

	DBPattern    string
	DsPattern    string
	TablePattern string
}

func newDefaultShardingSuite() ShardingSuite {
	m := []*masterSalvesDriver{
		{
			masterdsn: "root:root@tcp(localhost:13307)/order_detail_db_0",
			slavedsns: []string{"root:root@tcp(localhost:13308)/order_detail_db_0"},
		},
		{
			masterdsn: "root:root@tcp(localhost:13307)/order_detail_db_1",
			slavedsns: []string{"root:root@tcp(localhost:13308)/order_detail_db_1"},
		},
	}
	clusterDr := &clusterDriver{msDrivers: m}
	dbPattern := "order_detail_db_%d"
	dsPattern := "root:root@tcp(localhost:13307).%d"
	tablePattern := "order_detail_tab_%d"
	return ShardingSuite{
		driver: "mysql",
		algorithm: &hash.Hash{
			ShardingKey:  "OrderId",
			DBPattern:    &hash.Pattern{Name: dbPattern, Base: 2},
			TablePattern: &hash.Pattern{Name: tablePattern, Base: 3},
			DsPattern:    &hash.Pattern{Name: "root:root@tcp(localhost:13307).0", NotSharding: true},
		},
		DBPattern:    dbPattern,
		DsPattern:    dsPattern,
		TablePattern: tablePattern,
		clusters: &clusterDrivers{
			clDrivers: []*clusterDriver{clusterDr},
		},
	}
}

// TearDownSuite 会把所有的表清空
func (s *ShardingSuite) TearDownSuite() {
	t := s.T()
	dsts := s.algorithm.Broadcast(context.Background())
	for _, dst := range dsts {
		tbl := fmt.Sprintf("`%s`.`%s`", dst.DB, dst.Table)
		source, ok := s.dataSources[dst.Name]
		require.True(t, ok)
		_, err := source.Exec(context.Background(),
			datasource.Query{
				SQL: fmt.Sprintf("TRUNCATE TABLE %s", tbl),
				DB:  dst.DB,
			})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func (s *ShardingSuite) openDB(dvr, dsn string) (*sql.DB, error) {
	db, err := sql.Open(dvr, dsn)
	err = db.Ping()
	for err == driver.ErrBadConn {
		log.Printf("等待数据库启动...")
		err = db.Ping()
		time.Sleep(time.Second)
	}
	return db, err
}

func (s *ShardingSuite) initDB(r model.MetaRegistry) (*eorm.DB, error) {
	clDrivers := s.clusters.clDrivers
	sourceMap := make(map[string]datasource.DataSource, len(clDrivers))
	for i, clus := range clDrivers {
		msMap := make(map[string]*masterslave.MasterSlavesDB, 8)
		for j, d := range clus.msDrivers {
			master, err := s.openDB(s.driver, d.masterdsn)
			if err != nil {
				return nil, err
			}
			ss := make([]*sql.DB, 0, len(d.slavedsns))
			for _, slavedsn := range d.slavedsns {
				slave, err := s.openDB(s.driver, slavedsn)
				if err != nil {
					return nil, err
				}
				ss = append(ss, slave)
			}
			sl, err := roundrobin.NewSlaves(ss...)
			require.NoError(s.T(), err)
			s.slaves = &testBaseSlaves{Slaves: sl}
			masterSlaveDB := masterslave.NewMasterSlavesDB(
				master,
				masterslave.MasterSlavesWithSlaves(s.slaves),
			)
			dbName := fmt.Sprintf(s.DBPattern, j)
			msMap[dbName] = masterSlaveDB
		}
		sourceName := fmt.Sprintf(s.DsPattern, i)
		sourceMap[sourceName] = cluster.NewClusterDB(msMap)
	}
	s.dataSources = sourceMap
	dataSource := shardingsource.NewShardingDataSource(sourceMap)
	return eorm.OpenDS(s.driver, dataSource, eorm.DBOptionWithMetaRegistry(r))
}

func (s *ShardingSuite) SetupSuite() {
	t := s.T()
	r := model.NewMetaRegistry()
	_, err := r.Register(&test.OrderDetail{},
		model.WithTableShardingAlgorithm(s.algorithm))
	db, err := s.initDB(r)
	require.NoError(t, err)
	s.shardingDB = db
}
