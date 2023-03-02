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
	"log"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ecodeclub/eorm/internal/slaves/dns"

	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/model"
	"github.com/ecodeclub/eorm/internal/slaves"
	"github.com/ecodeclub/eorm/internal/slaves/roundrobin"
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
	orm, err := eorm.Open(s.driver, s.dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = orm.Wait(); err != nil {
		t.Fatal(err)
	}
	s.orm = orm
}

type masterSalvesDriver struct {
	masterdsn string
	slavedsns []string
}

type ShardingSuite struct {
	suite.Suite
	slaves     slaves.Slaves
	driver     string
	tbSet      map[string]bool
	driverMap  map[string]*masterSalvesDriver
	shardingDB *eorm.ShardingDB
	dbSf       model.ShardingAlgorithm
	tableSf    model.ShardingAlgorithm
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

func (s *ShardingSuite) initDB() (*eorm.ShardingDB, error) {
	masterSlaveDBs := make(map[string]*eorm.MasterSlavesDB, 8)
	for k, v := range s.driverMap {
		master, err := s.openDB(s.driver, v.masterdsn)
		if err != nil {
			return nil, err
		}
		ss := make([]*sql.DB, 0, len(v.slavedsns))
		for _, slavedsn := range v.slavedsns {
			slave, err := s.openDB(s.driver, slavedsn)
			if err != nil {
				return nil, err
			}
			ss = append(ss, slave)
		}
		sl, err := roundrobin.NewSlaves(ss...)
		require.NoError(s.T(), err)
		s.slaves = newTestSlaves(sl)
		masterSlaveDB, err := eorm.OpenMasterSlaveDB(
			s.driver, master, eorm.MasterSlaveWithSlaves(s.slaves))
		if err != nil {
			return nil, err
		}
		masterSlaveDBs[k] = masterSlaveDB
	}
	return eorm.OpenShardingDB(s.driver, masterSlaveDBs, eorm.ShardingDBOptionWithTables(s.tbSet))
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
	orm        *eorm.MasterSlavesDB
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

func (s *MasterSlaveSuite) initDb() (*eorm.MasterSlavesDB, error) {
	master, err := sql.Open(s.driver, s.masterDsn)
	if err != nil {
		return nil, err
	}
	ss, err := s.initSlaves(s.driver, s.slaveDsns...)
	if err != nil {
		return nil, err
	}
	s.testSlaves = newTestSlaves(ss)
	return eorm.OpenMasterSlaveDB(s.driver, master, eorm.MasterSlaveWithSlaves(s.testSlaves))

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

type testSlaves struct {
	slaves.Slaves
	ch chan string
}

func newTestSlaves(s slaves.Slaves) *testSlaves {
	return &testSlaves{
		Slaves: s,
		ch:     make(chan string, 1),
	}
}

func (s *testSlaves) Next(ctx context.Context) (slaves.Slave, error) {
	slave, err := s.Slaves.Next(ctx)
	if err != nil {
		return slave, err
	}
	s.ch <- slave.SlaveName
	return slave, err
}

type initSlaves func(driver string, slaveDsns ...string) (slaves.Slaves, error)

func newDnsSlaves(driver string, slaveDsns ...string) (slaves.Slaves, error) {
	return dns.NewSlaves(slaveDsns[0])
}

func newRoundRobinSlaves(driver string, slaveDsns ...string) (slaves.Slaves, error) {
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
