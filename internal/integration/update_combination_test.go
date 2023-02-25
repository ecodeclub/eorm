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

type UpdateCombinationTestSuite struct {
	Suite
}

func (u *UpdateCombinationTestSuite) SetupSuite() {
	u.Suite.SetupSuite()
	data1 := test.NewCombinedModel(1)
	res := eorm.NewInserter[test.CombinedModel](u.orm).Values(data1).Exec(context.Background())
	if res.Err() != nil {
		u.T().Fatal(res.Err())
	}
}

func (u *UpdateCombinationTestSuite) TearDownTest() {
	res := eorm.RawQuery[any](u.orm, "DELETE FROM `combined_model`").Exec(context.Background())
	if res.Err() != nil {
		u.T().Fatal(res.Err())
	}
}

func (u *UpdateCombinationTestSuite) TestUpdate() {
	testCases := []struct {
		name         string
		u            *eorm.Updater[test.CombinedModel]
		rowsAffected int64
		wantErr      error
	}{
		{
			name: "update columns",
			u: eorm.NewUpdater[test.CombinedModel](u.orm).Update(&test.CombinedModel{Age: 18}).
				Set(eorm.Columns("Age")).Where(eorm.C("Id").EQ(1)),
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
			affected, err := res.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.rowsAffected, affected)
		})
	}
}

func TestMySQL8UpdateCombination(t *testing.T) {
	suite.Run(t, &UpdateCombinationTestSuite{
		Suite{
			driver: "mysql",
			dsn:    "root:root@tcp(localhost:13306)/integration_test",
		},
	})
}
