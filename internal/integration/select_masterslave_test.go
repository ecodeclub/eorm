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
	"testing"
	"time"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave"

	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/test"
	"github.com/stretchr/testify/assert"
)

type MasterSlaveSelectTestSuite struct {
	MasterSlaveSuite
	data []*test.SimpleStruct
}

func (s *MasterSlaveSelectTestSuite) SetupSuite() {
	s.MasterSlaveSuite.SetupSuite()
	s.data = append(s.data, test.NewSimpleStruct(1))
	s.data = append(s.data, test.NewSimpleStruct(2))
	s.data = append(s.data, test.NewSimpleStruct(3))
	res := eorm.NewInserter[test.SimpleStruct](s.orm).Values(s.data...).Exec(context.Background())
	if res.Err() != nil {
		s.T().Fatal(res.Err())
	}
	// 避免主从延迟
	time.Sleep(time.Second * 10)
}

func (s *MasterSlaveSelectTestSuite) TearDownSuite() {
	res := eorm.RawQuery[any](s.orm, "TRUNCATE TABLE `simple_struct`").Exec(context.Background())
	if res.Err() != nil {
		s.T().Fatal(res.Err())
	}
}

func (s *MasterSlaveSelectTestSuite) TestMasterSlave() {
	testcases := []struct {
		name      string
		i         *eorm.Selector[test.SimpleStruct]
		wantErr   error
		wantRes   []*test.SimpleStruct
		wantSlave string
		ctx       func() context.Context
	}{
		{
			name:    "query use master",
			i:       eorm.NewSelector[test.SimpleStruct](s.orm).Where(eorm.C("Id").LT(4)),
			wantRes: s.data,
			ctx: func() context.Context {
				c := context.Background()
				return masterslave.UseMaster(c)
			},
		},
		// TODO 从库测试目前有查不到数据的bug
		{
			name:      "query use slave",
			i:         eorm.NewSelector[test.SimpleStruct](s.orm).Where(eorm.C("Id").LT(4)),
			wantSlave: "0",
			wantRes:   s.data,
			ctx: func() context.Context {
				return context.Background()
			},
		},
	}
	for _, tc := range testcases {
		s.T().Run(tc.name, func(t *testing.T) {
			ctx := tc.ctx()
			res, err := tc.i.GetMulti(ctx)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, res)
			slaveName := ""
			select {
			case slaveName = <-s.testSlaves.ch:
			default:
			}
			assert.Equal(t, tc.wantSlave, slaveName)
		})
	}
}

//func TestMasterSlaveSelect(t *testing.T) {
//	suite.Run(t, &MasterSlaveSelectTestSuite{
//		MasterSlaveSuite: MasterSlaveSuite{
//			driver:     "mysql",
//			masterDsn:  "root:root@tcp(localhost:13307)/integration_test",
//			slaveDsns:  []string{"root:root@tcp(localhost:13308)/integration_test"},
//			initSlaves: newRoundRobinSlaves,
//		},
//	})
//	suite.Run(t, &MasterSlaveDNSTestSuite{
//		MasterSlaveSuite: MasterSlaveSuite{
//			driver:     "mysql",
//			masterDsn:  "root:root@tcp(localhost:13307)/integration_test",
//			slaveDsns:  []string{"root:root@tcp(slave.a.com:13308)/integration_test"},
//			initSlaves: newDnsSlaves,
//		},
//	})
//}

type MasterSlaveDNSTestSuite struct {
	MasterSlaveSuite
	data []*test.SimpleStruct
}

func (m *MasterSlaveDNSTestSuite) SetupSuite() {
	m.MasterSlaveSuite.SetupSuite()
	m.data = append(m.data, test.NewSimpleStruct(1))
	m.data = append(m.data, test.NewSimpleStruct(2))
	m.data = append(m.data, test.NewSimpleStruct(3))
	res := eorm.NewInserter[test.SimpleStruct](m.orm).Values(m.data...).Exec(context.Background())
	if res.Err() != nil {
		m.T().Fatal(res.Err())
	}
	// 避免主从延迟
	time.Sleep(time.Second * 10)
}
func (m *MasterSlaveDNSTestSuite) TearDownSuite() {
	res := eorm.RawQuery[any](m.orm, "TRUNCATE TABLE `simple_struct`").Exec(context.Background())
	if res.Err() != nil {
		m.T().Fatal(res.Err())
	}
}

func (m *MasterSlaveDNSTestSuite) TestDNSMasterSlave() {
	testcases := []struct {
		name      string
		i         *eorm.Selector[test.SimpleStruct]
		wantErr   error
		wantRes   []*test.SimpleStruct
		wantSlave string
		ctx       func() context.Context
	}{
		// TODO 从库测试目前有查不到数据的bug
		{
			name:      "get slave with dns",
			i:         eorm.NewSelector[test.SimpleStruct](m.orm).Where(eorm.C("Id").LT(4)),
			wantSlave: "0",
			wantRes:   m.data,
			ctx: func() context.Context {
				return context.Background()
			},
		},
	}
	for _, tc := range testcases {
		m.T().Run(tc.name, func(t *testing.T) {
			ctx := tc.ctx()
			res, err := tc.i.GetMulti(ctx)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, res)
			slaveName := ""
			select {
			case slaveName = <-m.testSlaves.ch:
			default:
			}
			assert.Equal(t, tc.wantSlave, slaveName)
		})
	}
}
