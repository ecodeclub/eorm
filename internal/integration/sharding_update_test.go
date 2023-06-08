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

type ShardingUpdateTestSuite struct {
	ShardingSuite
	data []*test.OrderDetail
}

func (s *ShardingUpdateTestSuite) SetupSuite() {
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

func (s *ShardingUpdateTestSuite) TestShardingUpdater_Exec() {
	t := s.T()
	r := model.NewMetaRegistry()
	_, err := r.Register(&test.OrderDetail{},
		model.WithTableShardingAlgorithm(s.algorithm))
	require.NoError(t, err)
	eorm.DBOptionWithMetaRegistry(r)(s.shardingDB)
	testCases := []struct {
		name             string
		wantAffectedRows int64
		wantErr          error
		exec             sharding.Executor
		updatedSelector  *eorm.ShardingSelector[test.OrderDetail]
		selector         *eorm.ShardingSelector[test.OrderDetail]
		updatedQuerySet  []*test.OrderDetail
		querySet         []*test.OrderDetail
	}{
		{
			name: "where eq",
			exec: eorm.NewShardingUpdater[test.OrderDetail](s.shardingDB).Update(&test.OrderDetail{
				ItemId: 111, UsingCol1: "Jack", UsingCol2: "Jerry",
			}).Where(eorm.C("OrderId").EQ(123)),
			wantAffectedRows: 1,
			updatedSelector:  eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).Where(eorm.C("OrderId").EQ(123)),
			selector:         eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).Where(eorm.C("OrderId").NEQ(123)),
			updatedQuerySet: []*test.OrderDetail{
				{OrderId: 123, ItemId: 111, UsingCol1: "Jack", UsingCol2: "Jerry"},
			},
			querySet: []*test.OrderDetail{
				{8, 6, "Kobe", "Bryant"},
				{11, 8, "James", "Harden"},
				{234, 12, "Kevin", "Durant"},
				{253, 8, "Stephen", "Curry"},
				{181, 11, "Kawhi", "Leonard"},
			},
		},
		{
			name: "where or broadcast",
			exec: eorm.NewShardingUpdater[test.OrderDetail](s.shardingDB).Update(&test.OrderDetail{
				ItemId: 112, UsingCol1: "King", UsingCol2: "James",
			}).Set(eorm.Columns("ItemId", "UsingCol1", "UsingCol2")).
				Where(eorm.C("OrderId").EQ(123).Or(eorm.C("ItemId").EQ(11))),
			wantAffectedRows: 2,
			updatedSelector: eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
				Where(eorm.C("OrderId").In(123, 181)),
			selector: eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
				Where(eorm.C("OrderId").NotIn(123, 181)),
			updatedQuerySet: []*test.OrderDetail{
				{OrderId: 123, ItemId: 112, UsingCol1: "King", UsingCol2: "James"},
				{OrderId: 181, ItemId: 112, UsingCol1: "King", UsingCol2: "James"},
			},
			querySet: []*test.OrderDetail{
				{8, 6, "Kobe", "Bryant"},
				{11, 8, "James", "Harden"},
				{234, 12, "Kevin", "Durant"},
				{253, 8, "Stephen", "Curry"},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := tc.exec.Exec(context.Background())
			require.Equal(t, tc.wantErr, res.Err())
			if res.Err() != nil {
				return
			}

			affectRows, err := res.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.wantAffectedRows, affectRows)

			// TODO 从库测试目前有查不到数据的bug
			ctx := masterslave.UseMaster(context.Background())
			updatedQuerySet, err := tc.updatedSelector.GetMulti(ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.updatedQuerySet, updatedQuerySet)

			querySet, err := tc.selector.GetMulti(ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.querySet, querySet)

		})
	}
}

func (s *ShardingUpdateTestSuite) TearDownSuite() {
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

func TestMySQL8ShardingUpdate(t *testing.T) {
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
	suite.Run(t, &ShardingUpdateTestSuite{
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
