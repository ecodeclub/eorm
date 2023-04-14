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
//
//go:build e2e

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/model"
	operator "github.com/ecodeclub/eorm/internal/operator"
	"github.com/ecodeclub/eorm/internal/sharding"
	"github.com/ecodeclub/eorm/internal/sharding/hash"
	"github.com/ecodeclub/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ShardingSelectTestSuite struct {
	ShardingSuite
	data []*test.OrderDetail
}

func (s *ShardingSelectTestSuite) SetupSuite() {
	t := s.T()
	s.ShardingSuite.SetupSuite()
	for _, item := range s.data {
		shardingRes, err := s.algorithm.Sharding(
			context.Background(), sharding.Request{Op: operator.OpEQ, SkValues: map[string]any{"OrderId": item.OrderId}})
		require.NoError(t, err)
		require.NotNil(t, shardingRes.Dsts)
		for _, dst := range shardingRes.Dsts {
			tbl := fmt.Sprintf("`%s`.`%s`", dst.DB, dst.Table)
			sql := fmt.Sprintf("INSERT INTO %s (`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);", tbl)
			args := []any{item.OrderId, item.ItemId, item.UsingCol1, item.UsingCol2}
			source, ok := s.dataSources[dst.Name]
			require.True(t, ok)
			_, err := source.Exec(context.Background(), datasource.Query{SQL: sql, Args: args, DB: dst.DB})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	// 防止主从延迟
	time.Sleep(1)
}

func (s *ShardingSelectTestSuite) TestSardingSelectorGet() {
	t := s.T()
	r := model.NewMetaRegistry()
	_, err := r.Register(&test.OrderDetail{},
		model.WithTableShardingAlgorithm(s.algorithm))
	require.NoError(t, err)
	eorm.DBOptionWithMetaRegistry(r)(s.shardingDB)

	testCases := []struct {
		name    string
		s       *eorm.ShardingSelector[test.OrderDetail]
		wantErr error
		wantRes *test.OrderDetail
	}{
		{
			name: "found tab 1",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").EQ(123))
				return builder
			}(),
			wantRes: &test.OrderDetail{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
		},
		{
			name: "found tab 2",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").EQ(234))
				return builder
			}(),
			wantRes: &test.OrderDetail{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
		},
		{
			name: "found tab and",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").EQ(234).And(eorm.C("ItemId").EQ(12)))
				return builder
			}(),
			wantRes: &test.OrderDetail{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			// TODO 从库测试目前有查不到数据的bug
			ctx := masterslave.UseMaster(context.Background())
			res, err := tc.s.Get(ctx)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, res)
		})
	}
}

func (s *ShardingSelectTestSuite) TestSardingSelectorGetMulti() {
	t := s.T()
	r := model.NewMetaRegistry()
	_, err := r.Register(&test.OrderDetail{},
		model.WithTableShardingAlgorithm(s.algorithm))
	require.NoError(t, err)
	eorm.DBOptionWithMetaRegistry(r)(s.shardingDB)

	testCases := []struct {
		name    string
		s       *eorm.ShardingSelector[test.OrderDetail]
		wantErr error
		wantRes []*test.OrderDetail
	}{
		{
			name: "found tab or",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").EQ(123).Or(eorm.C("OrderId").EQ(234)))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			},
		},
		{
			name: "found tab or broadcast",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").EQ(123).Or(eorm.C("ItemId").EQ(12)))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			},
		},
		{
			name: "found tab or-and",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").EQ(234).
						Or(eorm.C("ItemId").EQ(10).And(eorm.C("OrderId").EQ(123))))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			},
		},
		{
			name: "found tab and-or",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").EQ(234).
						And(eorm.C("ItemId").EQ(12).Or(eorm.C("OrderId").EQ(123))))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			},
		},
		{
			name: "where gt",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").GT(1))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
			},
		},
		{
			name: "where lt",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").LT(150))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
			},
		},
		{
			name: "where gt eq",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").GTEQ(123))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
			},
		},
		{
			name: "where lt eq",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").LTEQ(123))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
			},
		},
		{
			name: "where in",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").In(8, 11, 123, 234))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			},
		},
		{
			name: "where eq or gt",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").EQ(8).
						Or(eorm.C("OrderId").GT(240)))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
			},
		},
		{
			name: "where in or eq",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").In(8, 11, 123).
						Or(eorm.C("OrderId").EQ(181)))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
			},
		},
		{
			name: "where in or gt",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").In(8, 11).
						Or(eorm.C("OrderId").GT(200)))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
			},
		},
		{
			name: "where between",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").GTEQ(123).And(eorm.C("OrderId").LTEQ(253)))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
			},
		},
		{
			name: "where eq and lt or gt",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					Where(eorm.C("OrderId").EQ(11).And(eorm.C("OrderId").LT(123)).
						Or(eorm.C("OrderId").GT(234)))
				return builder
			}(),
			wantRes: []*test.OrderDetail{
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			// TODO 从库测试目前有查不到数据的bug
			ctx := masterslave.UseMaster(context.Background())
			res, err := tc.s.GetMulti(ctx)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, tc.wantRes, res)
		})
	}
}

func (s *ShardingSelectTestSuite) TearDownSuite() {
	t := s.T()
	for _, item := range s.data {
		shardingRes, err := s.algorithm.Sharding(
			context.Background(), sharding.Request{Op: operator.OpEQ, SkValues: map[string]any{"OrderId": item.OrderId}})
		require.NoError(t, err)
		require.NotNil(t, shardingRes.Dsts)
		for _, dst := range shardingRes.Dsts {
			tbl := fmt.Sprintf("`%s`.`%s`", dst.DB, dst.Table)
			sql := fmt.Sprintf("DELETE FROM %s", tbl)
			source, ok := s.dataSources[dst.Name]
			require.True(t, ok)
			_, err := source.Exec(context.Background(), datasource.Query{SQL: sql, DB: dst.DB})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestMySQL8ShardingSelect(t *testing.T) {
	m := []*masterSalvesDriver{
		{
			masterdsn: "root:root@tcp(localhost:13307)/order_detail_db_0",
			slavedsns: []string{"root:root@tcp(localhost:13308)/order_detail_db_0"},
		},
		{
			masterdsn: "root:root@tcp(localhost:13307)/order_detail_db_1",
			slavedsns: []string{"root:root@tcp(localhost:13308)/order_detail_db_1"},
		},
	}
	clusterDr := &clusterDriver{msDrivers: m}
	suite.Run(t, &ShardingSelectTestSuite{
		ShardingSuite: ShardingSuite{
			driver: "mysql",
			algorithm: &hash.Hash{
				ShardingKey:  "OrderId",
				DBPattern:    &hash.Pattern{Name: "order_detail_db_%d", Base: 2},
				TablePattern: &hash.Pattern{Name: "order_detail_tab_%d", Base: 3},
				DsPattern:    &hash.Pattern{Name: "root:root@tcp(localhost:13307).0", NotSharding: true},
			},
			DBPattern: "order_detail_db_%d",
			DsPattern: "root:root@tcp(localhost:13307).%d",
			clusters: &clusterDrivers{
				clDrivers: []*clusterDriver{clusterDr},
			},
		},
		data: []*test.OrderDetail{
			{8, 6, "Kobe", "Bryant"},
			{11, 8, "James", "Harden"},
			{123, 10, "LeBron", "James"},
			{234, 12, "Kevin", "Durant"},
			{253, 8, "Stephen", "Curry"},
			{181, 11, "Kawhi", "Leonard"},
		},
	})
}
