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

//go:build e2e

package integration

import (
	"context"
	"database/sql"

	"github.com/gotomicro/eorm"
	"github.com/gotomicro/eorm/internal/slaves"
	"github.com/gotomicro/eorm/internal/slaves/roundrobin"
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
	s.slaveNamegeter = newSlaveNameGet(roundrobin.NewRoundrobin(slaves...))
	return eorm.OpenMasterSlaveDB(s.driver, master, eorm.MasterSlaveWithSlaveGeter(s.slaveNamegeter))

}

type slaveNamegeter struct {
	slaves.Slaves
	ch chan string
}

func newSlaveNameGet(geter slaves.Slaves) *slaveNamegeter {
	return &slaveNamegeter{
		Slaves: geter,
		ch:     make(chan string, 1),
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
