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
	"fmt"
	"testing"

	"github.com/gotomicro/eorm"
	"github.com/gotomicro/eorm/internal/model"
	"github.com/gotomicro/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ShardingSelectTestSuite struct {
	ShardingSuite
}

func (s *ShardingSelectTestSuite) SetupSuite() {
	t := s.T()
	s.ShardingSuite.SetupSuite()
	for dbName, db := range s.shardingDB.DBs {
		tblName := s.tbMap[dbName]
		tbl := fmt.Sprintf("`%s`.`%s`", dbName, tblName)
		sql := fmt.Sprintf("INSERT INTO %s (`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?),(?,?,?,?);", tbl)
		args := []any{123, 10, "LeBron", "James", 234, 12, "Kevin", "Durant"}
		res := eorm.RawQuery[any](db, sql, args...).Exec(context.Background())
		if res.Err() != nil {
			t.Fatal(res.Err())
		}
	}
}

func (s *ShardingSelectTestSuite) TestSelectorGet() {
	t := s.T()
	r := model.NewMetaRegistry()
	m, err := r.Register(&test.OrderDetail{},
		model.WithShardingKey("OrderId"),
		model.WithDBShardingFunc(func(skVal any) (string, error) {
			db := skVal.(int) / 100
			return fmt.Sprintf("order_detail_db_%d", db), nil
		}),
		model.WithTableShardingFunc(func(skVal any) (string, error) {
			tbl := skVal.(int) % 10
			return fmt.Sprintf("order_detail_tab_%d", tbl), nil
		}))
	require.NoError(t, err)
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
					RegisterTableMeta(m).Where(eorm.C("OrderId").EQ(123))
				return builder
			}(),
			wantRes: &test.OrderDetail{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
		},
		{
			name: "found tab 2",
			s: func() *eorm.ShardingSelector[test.OrderDetail] {
				builder := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
					RegisterTableMeta(m).Where(eorm.C("OrderId").EQ(234))
				return builder
			}(),
			wantRes: &test.OrderDetail{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
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

func (s *ShardingSelectTestSuite) TearDownSuite() {
	t := s.T()
	for dbName, db := range s.shardingDB.DBs {
		tblName := s.tbMap[dbName]
		tbl := fmt.Sprintf("`%s`.`%s`", dbName, tblName)
		sql := fmt.Sprintf("DELETE FROM %s", tbl)
		res := eorm.RawQuery[any](db, sql).Exec(context.Background())
		if res.Err() != nil {
			t.Fatal(res.Err())
		}
	}
}

func TestMySQL8ShardingSelect(t *testing.T) {
	shardingDB := &eorm.ShardingDB{
		DBs: make(map[string]*eorm.DB, 8),
	}
	m := map[string]*Driver{
		"order_detail_db_1": {driver: "mysql", dsn: "root:root@tcp(localhost:13306)/order_detail_db_1"},
		"order_detail_db_2": {driver: "mysql", dsn: "root:root@tcp(localhost:13306)/order_detail_db_2"},
	}
	tm := map[string]string{
		"order_detail_db_1": "order_detail_tab_3",
		"order_detail_db_2": "order_detail_tab_4",
	}
	suite.Run(t, &ShardingSelectTestSuite{
		ShardingSuite: ShardingSuite{
			tbMap:      tm,
			driverMap:  m,
			shardingDB: shardingDB,
		},
	})
}
