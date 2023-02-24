// Copyright 2021 ecodehub
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

	"github.com/ecodehub/eorm"
	"github.com/ecodehub/eorm/internal/model"
	"github.com/ecodehub/eorm/internal/test"
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
		skVal := item.OrderId
		dbName, err := s.dbSf(skVal)
		require.NoError(t, err)
		db := s.shardingDB.DBs[dbName]
		tblName, err := s.tableSf(skVal)
		require.NoError(t, err)
		tbl := fmt.Sprintf("`%s`.`%s`", dbName, tblName)
		sql := fmt.Sprintf("INSERT INTO %s (`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);", tbl)
		args := []any{item.OrderId, item.ItemId, item.UsingCol1, item.UsingCol2}
		res := eorm.RawQuery[any](db, sql, args...).Exec(context.Background())
		if res.Err() != nil {
			t.Fatal(res.Err())
		}
	}
	// TODO 防止®️主从延迟
	time.Sleep(1)
}

func (s *ShardingSelectTestSuite) TestSardingSelectorGet() {
	t := s.T()
	r := model.NewMetaRegistry()
	_, err := r.Register(&test.OrderDetail{},
		model.WithShardingKey("OrderId"),
		model.WithDBShardingFunc(s.dbSf),
		model.WithTableShardingFunc(s.tableSf))
	require.NoError(t, err)
	eorm.ShardingDBOptionWithMetaRegistry(r)(s.shardingDB)
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
			ctx := eorm.UseMaster(context.Background())
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
		model.WithShardingKey("OrderId"),
		model.WithDBShardingFunc(s.dbSf),
		model.WithTableShardingFunc(s.tableSf))
	require.NoError(t, err)
	eorm.ShardingDBOptionWithMetaRegistry(r)(s.shardingDB)
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
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			// TODO 从库测试目前有查不到数据的bug
			ctx := eorm.UseMaster(context.Background())
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
	for dbName, db := range s.shardingDB.DBs {
		for tblName, _ := range s.tbSet {
			tbl := fmt.Sprintf("`%s`.`%s`", dbName, tblName)
			sql := fmt.Sprintf("DELETE FROM %s", tbl)
			//ctx := eorm.CtxWithDBName(context.Background(), dbName)
			//// TODO 利用 ctx 传递 DB name
			//_, err := s.shardingDB.execContext(ctx, sql, args...)
			//if err != nil {
			//	t.Fatal(err)
			//}
			res := eorm.RawQuery[any](db, sql).Exec(context.Background())
			if res.Err() != nil {
				t.Fatal(res.Err())
			}
		}
	}
}

func TestMySQL8ShardingSelect(t *testing.T) {
	m := map[string]*masterSalvesDriver{
		"order_detail_db_1": {
			masterdsn: "root:root@tcp(localhost:13307)/order_detail_db_1",
			slavedsns: []string{"root:root@tcp(localhost:13308)/order_detail_db_1"},
		},
		"order_detail_db_2": {
			masterdsn: "root:root@tcp(localhost:13307)/order_detail_db_2",
			slavedsns: []string{"root:root@tcp(localhost:13308)/order_detail_db_2"},
		},
	}
	ts := map[string]bool{
		"order_detail_tab_3": true,
		"order_detail_tab_4": true,
	}
	suite.Run(t, &ShardingSelectTestSuite{
		ShardingSuite: ShardingSuite{
			driver:    "mysql",
			tbSet:     ts,
			driverMap: m,
			dbSf: func(skVal any) (string, error) {
				db := skVal.(int) / 100
				return fmt.Sprintf("order_detail_db_%d", db), nil
			},
			tableSf: func(skVal any) (string, error) {
				tbl := skVal.(int) % 10
				return fmt.Sprintf("order_detail_tab_%d", tbl), nil
			},
		},
		data: []*test.OrderDetail{
			{123, 10, "LeBron", "James"},
			{234, 12, "Kevin", "Durant"},
		},
	})
}
