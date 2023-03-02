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

	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type DeleteMasterSlaveTestSuite struct {
	MasterSlaveSuite
}

func (s *DeleteMasterSlaveTestSuite) SetupSuite() {
	s.MasterSlaveSuite.SetupSuite()
	data1 := test.NewSimpleStruct(1)
	data2 := test.NewSimpleStruct(2)
	data3 := test.NewSimpleStruct(3)
	res := eorm.NewInserter[test.SimpleStruct](s.orm).Values(data1, data2, data3).Exec(context.Background())
	if res.Err() != nil {
		s.T().Fatal(res.Err())
	}
}

func (s *DeleteMasterSlaveTestSuite) TearDownTest() {
	res := eorm.RawQuery[any](s.orm, "DELETE FROM `simple_struct`").Exec(context.Background())
	if res.Err() != nil {
		s.T().Fatal(res.Err())
	}
}

func (s *DeleteMasterSlaveTestSuite) TestDeleter() {
	testcases := []struct {
		name         string
		i            *eorm.Deleter[test.SimpleStruct]
		rowsAffected int64
		slaveName    string
		wantErr      error
	}{
		{
			name:         "delete",
			i:            eorm.NewDeleter[test.SimpleStruct](s.orm).From(&test.SimpleStruct{}).Where(eorm.C("Id").EQ("1")),
			rowsAffected: 1,
		},
	}
	for _, tc := range testcases {
		s.T().Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			res := tc.i.Exec(ctx)
			affected, err := res.RowsAffected()
			slaveName := ""
			select {
			case slaveName = <-s.testSlaves.ch:
			default:
			}
			require.Nil(t, err)
			assert.Equal(t, tc.rowsAffected, affected)
			assert.Equal(t, tc.slaveName, slaveName)
		})
	}

}

func TestMasterSlaveDelete(t *testing.T) {
	suite.Run(t, &DeleteMasterSlaveTestSuite{
		MasterSlaveSuite: MasterSlaveSuite{
			driver:     "mysql",
			masterDsn:  "root:root@tcp(localhost:13307)/integration_test",
			slaveDsns:  []string{"root:root@tcp(localhost:13308)/integration_test"},
			initSlaves: newRoundRobinSlaves,
		},
	})
}
