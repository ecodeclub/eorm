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
	"github.com/stretchr/testify/suite"
)

type SelectCombinationTestSuite struct {
	Suite
	data *test.CombinedModel
}

func (s *SelectCombinationTestSuite) SetupSuite() {
	s.Suite.SetupSuite()
	s.data = test.NewCombinedModel(1)
	res := eorm.NewInserter[test.CombinedModel](s.orm).Values(s.data).Exec(context.Background())
	if res.Err() != nil {
		s.T().Fatal(res.Err())
	}
}

func (s *SelectCombinationTestSuite) TearDownSuite() {
	res := eorm.RawQuery[any](s.orm, "DELETE FROM `combined_model`").Exec(context.Background())
	if res.Err() != nil {
		s.T().Fatal(res.Err())
	}
}

func (s *SelectCombinationTestSuite) TestGet() {
	testCases := []struct {
		name    string
		s       *eorm.Selector[test.CombinedModel]
		wantErr error
		wantRes *test.CombinedModel
	}{
		{
			name:    "not found",
			s:       eorm.NewSelector[test.CombinedModel](s.orm).Where(eorm.C("Id").EQ(9)),
			wantErr: eorm.ErrNoRows,
		},
		{
			name:    "found",
			s:       eorm.NewSelector[test.CombinedModel](s.orm).Where(eorm.C("Id").EQ(1)),
			wantRes: s.data,
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			res, err := tc.s.Get(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, res)
		})
	}
}

func TestMySQL8SelectCombination(t *testing.T) {
	suite.Run(t, &SelectCombinationTestSuite{
		Suite: Suite{
			driver: "mysql",
			dsn:    "root:root@tcp(localhost:13306)/integration_test",
		},
	})
}
