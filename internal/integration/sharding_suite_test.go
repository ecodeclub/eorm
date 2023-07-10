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
	"testing"
	"time"

	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/cluster"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves/roundrobin"
	"github.com/ecodeclub/eorm/internal/datasource/shardingsource"
	"github.com/ecodeclub/eorm/internal/model"
	operator "github.com/ecodeclub/eorm/internal/operator"
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
	ShardingKey  string
}

func newDefaultShardingSuite() ShardingSuite {
	m := []*masterSalvesDriver{
		{
			masterdsn: "root:root@tcp(localhost:13307)/order_detail_db_0?multiStatements=true&interpolateParams=true",
			slavedsns: []string{"root:root@tcp(localhost:13308)/order_detail_db_0?multiStatements=true&interpolateParams=true"},
		},
		{
			masterdsn: "root:root@tcp(localhost:13307)/order_detail_db_1?multiStatements=true&interpolateParams=true",
			slavedsns: []string{"root:root@tcp(localhost:13308)/order_detail_db_1?multiStatements=true&interpolateParams=true"},
		},
	}
	clusterDr := &clusterDriver{msDrivers: m}
	dbPattern := "order_detail_db_%d"
	dsPattern := "root:root@tcp(localhost:13307).%d"
	tablePattern := "order_detail_tab_%d"
	return ShardingSuite{
		driver:       "mysql",
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
	return eorm.OpenDS(s.driver, dataSource, eorm.DBWithMetaRegistry(r))
}

func (s *ShardingSuite) SetupSuite() {
	t := s.T()
	r := model.NewMetaRegistry()
	sk := "OrderId"
	s.algorithm = &hash.Hash{
		ShardingKey:  sk,
		DBPattern:    &hash.Pattern{Name: s.DBPattern, Base: 2},
		TablePattern: &hash.Pattern{Name: s.TablePattern, Base: 3},
		DsPattern:    &hash.Pattern{Name: "root:root@tcp(localhost:13307).0", NotSharding: true},
	}
	s.ShardingKey = sk
	_, err := r.Register(&test.OrderDetail{},
		model.WithTableShardingAlgorithm(s.algorithm))
	db, err := s.initDB(r)
	require.NoError(t, err)
	s.shardingDB = db
}

type ShardingSelectUpdateInsertSuite struct {
	ShardingSuite
	data []*test.OrderDetail
}

func newShardingSelectUpdateInsertSuite() ShardingSelectUpdateInsertSuite {
	return ShardingSelectUpdateInsertSuite{
		ShardingSuite: newDefaultShardingSuite(),
		data: []*test.OrderDetail{
			{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
			{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
			{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
			{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
			{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
		},
	}
}

func (s *ShardingSelectUpdateInsertSuite) SetupSuite() {
	t := s.T()
	s.ShardingSuite.SetupSuite()
	for _, item := range s.data {
		shardingRes, err := s.algorithm.Sharding(
			context.Background(), sharding.Request{Op: operator.OpEQ, SkValues: map[string]any{s.ShardingKey: item.OrderId}})
		require.NoError(t, err)
		require.NotNil(t, shardingRes.Dsts)
		for _, dst := range shardingRes.Dsts {
			tbl := fmt.Sprintf("`%s`.`%s`", dst.DB, dst.Table)
			sql := fmt.Sprintf("INSERT INTO %s (`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);", tbl)
			args := []any{item.OrderId, item.ItemId, item.UsingCol1, item.UsingCol2}
			source, ok := s.dataSources[dst.Name]
			require.True(t, ok)
			_, err := source.Exec(context.Background(), datasource.Query{SQL: sql, Args: args, DB: dst.DB})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	// 防止主从延迟
	time.Sleep(1)
}

func (s *ShardingSelectUpdateInsertSuite) findTgt(t *testing.T, values []*test.OrderDetail) []*test.OrderDetail {
	od := values[0]
	pre := eorm.C(s.ShardingKey).EQ(od.OrderId)
	for i := 1; i < len(values); i++ {
		od = values[i]
		pre = pre.Or(eorm.C(s.ShardingKey).EQ(od.OrderId))
	}
	querySet, err := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
		Where(pre).GetMulti(masterslave.UseMaster(context.Background()))
	require.NoError(t, err)
	return querySet
}

func (s *ShardingSelectUpdateInsertSuite) TearDownSuite() {
	t := s.T()
	for _, item := range s.data {
		shardingRes, err := s.algorithm.Sharding(
			context.Background(), sharding.Request{Op: operator.OpEQ, SkValues: map[string]any{"OrderId": item.OrderId}})
		require.NoError(t, err)
		require.NotNil(t, shardingRes.Dsts)
		for _, dst := range shardingRes.Dsts {
			tbl := fmt.Sprintf("`%s`.`%s`", dst.DB, dst.Table)
			sql := fmt.Sprintf("DELETE FROM %s", tbl)
			source, ok := s.dataSources[dst.Name]
			require.True(t, ok)
			_, err := source.Exec(context.Background(), datasource.Query{SQL: sql, DB: dst.DB})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}
