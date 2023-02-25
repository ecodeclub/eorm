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

	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type MasterSlaveSelectTestSuite struct {
	MasterSlaveSuite
	data []*test.SimpleStruct
}

func (m *MasterSlaveSelectTestSuite) SetupSuite() {
	m.MasterSlaveSuite.SetupSuite()
	m.data = append(m.data, test.NewSimpleStruct(1))
	m.data = append(m.data, test.NewSimpleStruct(2))
	m.data = append(m.data, test.NewSimpleStruct(3))
	res := eorm.NewInserter[test.SimpleStruct](m.orm).Values(m.data...).Exec(context.Background())
	if res.Err() != nil {
		m.T().Fatal(res.Err())
		m.T()
	}
}

func (s *MasterSlaveSelectTestSuite) TearDownSuite() {
	res := eorm.RawQuery[any](s.orm, "DELETE FROM `simple_struct`").Exec(context.Background())
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
				c = eorm.UseMaster(c)
				return c
			},
		},
		// TODO 从库测试目前有查不到数据的bug
		//{
		//	name:      "query use slave",
		//	i:         eorm.NewSelector[test.SimpleStruct](s.orm).Where(eorm.C("Id").LT(4)),
		//	wantSlave: "0",
		//	wantRes:   s.data,
		//	ctx: func() context.Context {
		//		return context.Background()
		//	},
		//},
	}
	for _, tc := range testcases {
		s.T().Run(tc.name, func(t *testing.T) {
			ctx := tc.ctx()
			time.Sleep(time.Second)
			res, err := tc.i.GetMulti(ctx)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, res)
			slaveName := ""
			select {
			case slaveName = <-s.slaveNamegeter.ch:
			default:
			}
			assert.Equal(t, tc.wantSlave, slaveName)
		})
	}
}

func TestMasterSlaveSelect(t *testing.T) {
	suite.Run(t, &MasterSlaveSelectTestSuite{
		MasterSlaveSuite: MasterSlaveSuite{
			driver:    "mysql",
			masterdsn: "root:root@tcp(localhost:13307)/integration_test",
			slavedsns: []string{"root:root@tcp(localhost:13308)/integration_test"},
		},
	})
}
