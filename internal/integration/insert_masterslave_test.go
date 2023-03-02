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

type InsertMasterSlaveTestSuite struct {
	MasterSlaveSuite
}

func (i *InsertMasterSlaveTestSuite) TearDownTest() {
	res := eorm.RawQuery[any](i.orm, "DELETE FROM `simple_struct`").Exec(context.Background())
	if res.Err() != nil {
		i.T().Fatal(res.Err())
	}
}

func (i *InsertMasterSlaveTestSuite) TestInsert() {
	testcases := []struct {
		name         string
		i            *eorm.Inserter[test.SimpleStruct]
		slaveName    string
		rowsAffected int64
		wantErr      error
	}{
		{
			name:         "",
			i:            eorm.NewInserter[test.SimpleStruct](i.orm).Values(&test.SimpleStruct{Id: 1}),
			rowsAffected: 1,
		},
	}
	for _, tc := range testcases {
		i.T().Run(tc.name, func(t *testing.T) {
			res := tc.i.Exec(context.Background())
			assert.Equal(t, tc.wantErr, res.Err())
			if res.Err() != nil {
				return
			}
			slaveName := ""
			select {
			case slaveName = <-i.testSlaves.ch:
			default:
			}
			affected, err := res.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.rowsAffected, affected)
			assert.Equal(t, tc.slaveName, slaveName)
		})
	}
}
func TestMasterSlaveInsert(t *testing.T) {
	suite.Run(t, &InsertMasterSlaveTestSuite{
		MasterSlaveSuite: MasterSlaveSuite{
			driver:     "mysql",
			masterDsn:  "root:root@tcp(localhost:13307)/integration_test",
			slaveDsns:  []string{"root:root@tcp(localhost:13308)/integration_test"},
			initSlaves: newRoundRobinSlaves,
		},
	})
}
