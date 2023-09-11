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

package transaction_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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

type ShardingTransactionSuite struct {
	shardingKey string
	shardingDB  *eorm.DB
	algorithm   sharding.Algorithm

	suite.Suite
	datasource.DataSource
	clusterDB     datasource.DataSource
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

func (s *ShardingTransactionSuite) SetupTest() {
	t := s.T()
	s.initMock(t)
}

func (s *ShardingTransactionSuite) initMock(t *testing.T) {
	var err error
	s.mockMaster1DB, s.mockMaster, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockSlave1DB, s.mockSlave1, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockSlave2DB, s.mockSlave2, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockSlave3DB, s.mockSlave3, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	s.mockMaster2DB, s.mockMaster2, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockSlave4DB, s.mockSlave4, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockSlave5DB, s.mockSlave5, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockSlave6DB, s.mockSlave6, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	db1 := masterslave.NewMasterSlavesDB(s.mockMaster1DB, masterslave.MasterSlavesWithSlaves(
		newSlaves(t, s.mockSlave1DB, s.mockSlave2DB, s.mockSlave3DB)))

	db2 := masterslave.NewMasterSlavesDB(s.mockMaster2DB, masterslave.MasterSlavesWithSlaves(
		newSlaves(t, s.mockSlave4DB, s.mockSlave5DB, s.mockSlave6DB)))

	s.clusterDB = cluster.NewClusterDB(map[string]*masterslave.MasterSlavesDB{
		"order_detail_db_0": db1,
		"order_detail_db_1": db2,
	})

	s.DataSource = shardingsource.NewShardingDataSource(map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": s.clusterDB,
	})

	r := model.NewMetaRegistry()
	sk := "OrderId"
	s.algorithm = &hash.Hash{
		ShardingKey:  sk,
		DBPattern:    &hash.Pattern{Name: "order_detail_db_%d", Base: 2},
		TablePattern: &hash.Pattern{Name: "order_detail_tab_%d", Base: 3},
		DsPattern:    &hash.Pattern{Name: "0.db.cluster.company.com:3306", NotSharding: true},
	}
	s.shardingKey = sk
	_, err = r.Register(&test.OrderDetail{},
		model.WithTableShardingAlgorithm(s.algorithm))
	require.NoError(t, err)
	db, err := eorm.OpenDS("mysql", s.DataSource, eorm.DBWithMetaRegistry(r))
	require.NoError(t, err)
	s.shardingDB = db
}

func (s *ShardingTransactionSuite) TearDownTest() {
	_ = s.mockMaster1DB.Close()
	_ = s.mockSlave1DB.Close()
	_ = s.mockSlave2DB.Close()
	_ = s.mockSlave3DB.Close()

	_ = s.mockMaster2DB.Close()
	_ = s.mockSlave4DB.Close()
	_ = s.mockSlave5DB.Close()
	_ = s.mockSlave6DB.Close()
}

func (s *ShardingTransactionSuite) findTgt(t *testing.T, values []*test.OrderDetail) []*test.OrderDetail {
	od := values[0]
	pre := eorm.C(s.shardingKey).EQ(od.OrderId)
	for i := 1; i < len(values); i++ {
		od = values[i]
		pre = pre.Or(eorm.C(s.shardingKey).EQ(od.OrderId))
	}
	querySet, err := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
		Where(pre).GetMulti(masterslave.UseMaster(context.Background()))
	require.NoError(t, err)
	return querySet
}

func newShardingTransactionSuite() ShardingTransactionSuite {
	return ShardingTransactionSuite{}
}

func newSlaves(t *testing.T, dbs ...*sql.DB) slaves.Slaves {
	res, err := roundrobin.NewSlaves(dbs...)
	require.NoError(t, err)
	return res
}

func newMockCommitErr(dbName string, err error) error {
	return fmt.Errorf("masterslave DB name [%s] Commit error: %w", dbName, err)
}

func newMockRollbackErr(dbName string, err error) error {
	return fmt.Errorf("masterslave DB name [%s] Rollback error: %w", dbName, err)
}
