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
	"github.com/gotomicro/eorm"
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

type Driver struct {
	driver string
	dsn    string
}

type ShardingSuite struct {
	suite.Suite
	tbMap      map[string]string
	driverMap  map[string]*Driver
	shardingDB *eorm.ShardingDB
}

func (s *ShardingSuite) SetupSuite() {
	t := s.T()
	for k, v := range s.driverMap {
		orm, err := eorm.Open(v.driver, v.dsn)
		if err != nil {
			t.Fatal(err)
		}
		if err = orm.Wait(); err != nil {
			t.Fatal(err)
		}
		s.shardingDB.DBs[k] = orm
	}
}
