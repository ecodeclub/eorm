// Copyright 2021 ecodehub
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

	"github.com/ecodehub/eorm"
	"github.com/ecodehub/eorm/internal/model"
	"github.com/ecodehub/eorm/internal/slaves"
	"github.com/ecodehub/eorm/internal/slaves/roundrobin"
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
	*baseSlaveNamegeter
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
		slaves := make([]*sql.DB, 0, len(v.slavedsns))
		for _, slavedsn := range v.slavedsns {
			slave, err := s.openDB(s.driver, slavedsn)
			if err != nil {
				return nil, err
			}
			slaves = append(slaves, slave)
		}
		s.baseSlaveNamegeter = &baseSlaveNamegeter{
			Slaves: roundrobin.NewSlaves(slaves...),
		}
		masterSlaveDB, err := eorm.OpenMasterSlaveDB(
			s.driver, master, eorm.MasterSlaveWithSlaves(s.baseSlaveNamegeter))
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
	driver    string
	masterdsn string
	slavedsns []string
	orm       *eorm.MasterSlavesDB
	*slaveNamegeter
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
	master, err := sql.Open(s.driver, s.masterdsn)
	if err != nil {
		return nil, err
	}
	slaves := make([]*sql.DB, 0, len(s.slavedsns))
	for _, slavedsn := range s.slavedsns {
		slave, err := sql.Open(s.driver, slavedsn)
		if err != nil {
			return nil, err
		}
		slaves = append(slaves, slave)
	}
	s.slaveNamegeter = newSlaveNameGet(roundrobin.NewSlaves(slaves...))
	return eorm.OpenMasterSlaveDB(s.driver, master, eorm.MasterSlaveWithSlaves(s.slaveNamegeter))

}

type baseSlaveNamegeter struct {
	slaves.Slaves
}

func (s *baseSlaveNamegeter) Next(ctx context.Context) (slaves.Slave, error) {
	slave, err := s.Slaves.Next(ctx)
	if err != nil {
		return slave, err
	}
	return slave, err
}

type slaveNamegeter struct {
	*baseSlaveNamegeter
	ch chan string
}

func newSlaveNameGet(geter slaves.Slaves) *slaveNamegeter {
	return &slaveNamegeter{
		baseSlaveNamegeter: &baseSlaveNamegeter{
			Slaves: geter,
		},
		ch: make(chan string, 1),
	}
}

func (s *slaveNamegeter) Next(ctx context.Context) (slaves.Slave, error) {
	slave, err := s.Slaves.Next(ctx)
	if err != nil {
		return slave, err
	}
	s.ch <- slave.SlaveName
	return slave, err
}
