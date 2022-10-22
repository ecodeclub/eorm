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
	"github.com/gotomicro/eorm"
	"github.com/gotomicro/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

type UpdateTestSuite struct {
	Suite
}

func (u *UpdateTestSuite) SetupSuite() {
	u.Suite.SetupSuite()
	data1 := test.NewSimpleStruct(1)
	_, err := eorm.NewInserter[test.SimpleStruct](u.orm).Values(data1).Exec(context.Background())
	if err != nil {
		u.T().Fatal(err)
	}
}

func (u *UpdateTestSuite) TearDownTest() {
	_, err := eorm.RawQuery[any](u.orm, "DELETE FROM `simple_struct`").Exec(context.Background())
	if err != nil {
		u.T().Fatal(err)
	}
}

func (u *UpdateTestSuite) TestUpdate() {
	testCases := []struct {
		name         string
		u            *eorm.Updater[test.SimpleStruct]
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
			res, err := tc.u.Exec(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			affected, err := res.RowsAffected()
			assert.Equal(t, tc.rowsAffected, affected)
		})
	}
}

func TestMySQL8Update(t *testing.T) {
	suite.Run(t, &UpdateTestSuite{
		Suite{
			driver: "mysql",
			dsn:    "root:root@tcp(localhost:13306)/integration_test",
		},
	})
}
