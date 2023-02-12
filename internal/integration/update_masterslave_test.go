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
	"testing"

	"github.com/gotomicro/eorm"
	"github.com/gotomicro/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type UpdateMasterSlaveTestSuite struct {
	MasterSlaveSuite
}

func (u *UpdateMasterSlaveTestSuite) SetupSuite() {
	u.MasterSlaveSuite.SetupSuite()
	data1 := test.NewSimpleStruct(1)
	res := eorm.NewInserter[test.SimpleStruct](u.orm).Values(data1).Exec(context.Background())
	if res.Err() != nil {
		u.T().Fatal(res.Err())
	}
}

func (u *UpdateMasterSlaveTestSuite) TearDownTest() {
	res := eorm.RawQuery[any](u.orm, "DELETE FROM `simple_struct`").Exec(context.Background())
	if res.Err() != nil {
		u.T().Fatal(res.Err())
	}
}

func (u *UpdateMasterSlaveTestSuite) TestUpdate() {
	testCases := []struct {
		name         string
		u            *eorm.Updater[test.SimpleStruct]
		slaveName    string
		rowsAffected int64
		wantErr      error
	}{
		{
			name: "update columns",
			u: eorm.NewUpdater[test.SimpleStruct](u.orm).Update(&test.SimpleStruct{Int: 18}).
				Set(eorm.Columns("Int")).Where(eorm.C("Id").EQ(1)),
			rowsAffected: 1,
		},
	}
	for _, tc := range testCases {
		u.T().Run(tc.name, func(t *testing.T) {
			res := tc.u.Exec(context.Background())
			assert.Equal(t, tc.wantErr, res.Err())
			if res.Err() != nil {
				return
			}
			slaveName := ""
			select {
			case slaveName = <-u.slaveNamegeter.ch:
			default:
			}
			affected, err := res.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.rowsAffected, affected)
			assert.Equal(t, tc.slaveName, slaveName)
		})
	}
}

func TestMasterSlaveUpdate(t *testing.T) {
	suite.Run(t, &UpdateMasterSlaveTestSuite{
		MasterSlaveSuite: MasterSlaveSuite{
			driver:    "mysql",
			masterdsn: "root:root@tcp(localhost:13307)/integration_test",
			slavedsns: []string{"root:root@tcp(localhost:13308)/integration_test"},
		},
	})
}
