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

	_ "github.com/go-sql-driver/mysql"
	"github.com/gotomicro/eorm"
	"github.com/gotomicro/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type DeleteTestSuite struct {
	Suite
}

func (s *DeleteTestSuite) SetupSuite() {
	s.Suite.SetupSuite()
	data1 := test.NewSimpleStruct(1)
	data2 := test.NewSimpleStruct(2)
	data3 := test.NewSimpleStruct(3)
	_, err := eorm.NewInserter[test.SimpleStruct](s.orm).Values(data1, data2, data3).Exec(context.Background())
	if err != nil {
		s.T().Fatal(err)
	}
}

func (i *DeleteTestSuite) TearDownTest() {
	_, err := eorm.RawQuery[any](i.orm, "DELETE FROM `simple_struct`").Exec(context.Background())
	if err != nil {
		i.T().Fatal(err)
	}
}

func (i *DeleteTestSuite) TestDeleter() {
	testCases := []struct {
		name         string
		i            *eorm.Deleter[test.SimpleStruct]
		rowsAffected int64
		wantErr      error
	}{
		{
			name:         "id only",
			i:            eorm.NewDeleter[test.SimpleStruct](i.orm).From(&test.SimpleStruct{}).Where(eorm.C("Id").EQ("1")),
			rowsAffected: 1,
		},
		{
			name:         "all field",
			i:            eorm.NewDeleter[test.SimpleStruct](i.orm).From(&test.SimpleStruct{}),
			rowsAffected: 2,
		},
	}
	for _, tc := range testCases {
		i.T().Run(tc.name, func(t *testing.T) {
			res, err := tc.i.Exec(context.Background())
			require.Equal(t, tc.wantErr, err)
			affected, err := res.RowsAffected()
			require.NotNil(t, err)
			assert.Equal(t, tc.rowsAffected, affected)
		})
	}
}

func TestMySQL8tDelete(t *testing.T) {
	suite.Run(t, &DeleteTestSuite{
		Suite{
			driver: "mysql",
			dsn:    "root:root@tcp(localhost:13306)/integration_test",
		},
	})
}
