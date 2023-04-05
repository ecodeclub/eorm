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

	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves/roundrobin"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	slaves2 "github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves/dns"

	"github.com/ecodeclub/eorm/internal/datasource/shardingsource"

	"github.com/ecodeclub/eorm/internal/datasource/single"

	"github.com/stretchr/testify/require"

	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/cluster"
	"github.com/ecodeclub/eorm/internal/sharding"
	"github.com/stretchr/testify/suite"
)

type Suite struct {
	suite.Suite
	driver string
	dsn    string
	orm    *eorm.DB
}

func (s *Suite) SetupSuite() {
	t := s.T()
	db, err := single.OpenDB(s.driver, s.dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Wait(); err != nil {
		t.Fatal(err)
	}
	orm, err := eorm.OpenDS(s.driver, db)
	if err != nil {
		t.Fatal(err)
	}
	s.orm = orm
}

type clusterDrivers struct {
	clDrivers []*clusterDriver
}

type clusterDriver struct {
	msDrivers []*masterSalvesDriver
}

type masterSalvesDriver struct {
	masterdsn string
	slavedsns []string
}

type ShardingSuite struct {
	suite.Suite
	slaves      slaves2.Slaves
	clusters    *clusterDrivers
	shardingDB  *eorm.DB
	algorithm   sharding.Algorithm
	dataSources map[string]datasource.DataSource
	driver      string
	DBPattern   string
	DsPattern   string
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

func (s *ShardingSuite) initDB() (*eorm.DB, error) {
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
				master, masterslave.MasterSlavesWithSlaves(s.slaves))
			dbName := fmt.Sprintf(s.DBPattern, j)
			msMap[dbName] = masterSlaveDB
		}
		sourceName := fmt.Sprintf(s.DsPattern, i)
		sourceMap[sourceName] = cluster.NewClusterDB(msMap)
	}
	s.dataSources = sourceMap
	dataSource := shardingsource.NewShardingDataSource(sourceMap)
	return eorm.OpenDS(s.driver, dataSource)
}

func (s *ShardingSuite) SetupSuite() {
	t := s.T()
	db, err := s.initDB()
	if err != nil {
		t.Fatal(err)
	}
	s.shardingDB = db
}

type MasterSlaveSuite struct {
	suite.Suite
	driver     string
	masterDsn  string
	slaveDsns  []string
	orm        *eorm.DB
	initSlaves initSlaves
	*testSlaves
}

func (s *MasterSlaveSuite) SetupSuite() {
	t := s.T()
	orm, err := s.initDb()
	if err != nil {
		t.Fatal(err)
	}
	s.orm = orm
}

func (s *MasterSlaveSuite) initDb() (*eorm.DB, error) {
	master, err := sql.Open(s.driver, s.masterDsn)
	if err != nil {
		return nil, err
	}
	ss, err := s.initSlaves(s.driver, s.slaveDsns...)
	if err != nil {
		return nil, err
	}
	s.testSlaves = newTestSlaves(ss)
	return eorm.OpenDS(s.driver, masterslave.NewMasterSlavesDB(master, masterslave.MasterSlavesWithSlaves(s.testSlaves)))

}

//type baseSlaveNamegeter struct {
//	slaves.Slaves
//}
//
//func (s *baseSlaveNamegeter) Next(ctx context.Context) (slaves.Slave, error) {
//	slave, err := s.Slaves.Next(ctx)
//	if err != nil {
//		return slave, err
//	}
//	return slave, err
//}

type testBaseSlaves struct {
	slaves2.Slaves
}

func (s *testBaseSlaves) Next(ctx context.Context) (slaves2.Slave, error) {
	slave, err := s.Slaves.Next(ctx)
	if err != nil {
		return slave, err
	}
	return slave, err
}

type testSlaves struct {
	*testBaseSlaves
	ch chan string
}

func newTestSlaves(s slaves2.Slaves) *testSlaves {
	return &testSlaves{
		testBaseSlaves: &testBaseSlaves{
			Slaves: s,
		},
		ch: make(chan string, 1),
	}
}

func (s *testSlaves) Next(ctx context.Context) (slaves2.Slave, error) {
	slave, err := s.Slaves.Next(ctx)
	if err != nil {
		return slave, err
	}
	s.ch <- slave.SlaveName
	return slave, err
}

type initSlaves func(driver string, slaveDsns ...string) (slaves2.Slaves, error)

func newDnsSlaves(driver string, slaveDsns ...string) (slaves2.Slaves, error) {
	return dns.NewSlaves(slaveDsns[0])
}

func newRoundRobinSlaves(driver string, slaveDsns ...string) (slaves2.Slaves, error) {
	ss := make([]*sql.DB, 0, len(slaveDsns))
	for _, slaveDsn := range slaveDsns {
		slave, err := sql.Open(driver, slaveDsn)
		if err != nil {
			return nil, err
		}
		ss = append(ss, slave)
	}
	return roundrobin.NewSlaves(ss...)
}
