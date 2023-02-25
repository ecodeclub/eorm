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
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type InsertCompositionTestSuite struct {
	Suite
}

func (i *InsertCompositionTestSuite) TearDownTest() {
	res := eorm.RawQuery[any](i.orm, "DELETE FROM `combined_model`").Exec(context.Background())
	if res.Err() != nil {
		i.T().Fatal(res.Err())
	}
}

func (i *InsertCompositionTestSuite) TestInsert() {
	testCases := []struct {
		name         string
		i            *eorm.Inserter[test.CombinedModel]
		rowsAffected int64
		wantErr      error
	}{
		{
			name:         "id only",
			i:            eorm.NewInserter[test.CombinedModel](i.orm).Values(&test.CombinedModel{Id: 1}),
			rowsAffected: 1,
		},
		{
			name:         "all field",
			i:            eorm.NewInserter[test.CombinedModel](i.orm).Values(test.NewCombinedModel(2)),
			rowsAffected: 1,
		},
		{
			name:         "ignore pk",
			i:            eorm.NewInserter[test.CombinedModel](i.orm).SkipPK().Values(test.NewCombinedModel(3)),
			rowsAffected: 1,
		},
		{
			name: "ignore pk multi",
			i: eorm.NewInserter[test.CombinedModel](i.orm).SkipPK().
				Values(test.NewCombinedModel(4), test.NewCombinedModel(5)),
			rowsAffected: 2,
		},
	}
	for _, tc := range testCases {
		i.T().Run(tc.name, func(t *testing.T) {
			res := tc.i.Exec(context.Background())
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

func TestMySQL8InsertComposition(t *testing.T) {
	suite.Run(t, &InsertCompositionTestSuite{
		Suite{
			driver: "mysql",
			dsn:    "root:root@tcp(localhost:13306)/integration_test",
		},
	})
}
