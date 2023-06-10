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
	"fmt"
	"testing"
	"time"

	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ShardingInsertTestSuite struct {
	ShardingSuite
}

func (s *ShardingInsertTestSuite) TestInsert() {
	t := s.T()
	// db 是 2 分库
	// table 是 3 分表
	testCases := []struct {
		name   string
		values []*test.OrderDetail

		// 用来验证对应的数据库表里面有数据
		// 其它数据库表没有数据
		after        func(t *testing.T)
		wantErr      error
		wantAffected int64
	}{
		{
			name: "插入单条数据",
			values: []*test.OrderDetail{
				// order_detail_db_1.order_detail_tab_1
				{
					OrderId: 1,
					ItemId:  1,
				},
			},

			// 受影响行数是1，然后又找到了这个数据，所以可以确保没有插入别的表
			after: func(t *testing.T) {
				od := s.findTgt(t, 1)
				assert.Equal(t, test.OrderDetail{
					OrderId: 1,
					ItemId:  1,
				}, od)
			},
			wantAffected: 1,
		},
		{
			name: "同库不同表",
			values: []*test.OrderDetail{
				{
					// order_detail_db_1.order_detail_tab_0
					OrderId: 3,
					ItemId:  1,
				},
				{
					// order_detail_db_1.order_detail_tab_0
					OrderId: 5,
					ItemId:  1,
				},
			},
			after: func(t *testing.T) {
				od3 := s.findTgt(t, 3)
				assert.Equal(t, test.OrderDetail{
					OrderId: 3,
					ItemId:  1,
				}, od3)
				od5 := s.findTgt(t, 5)
				assert.Equal(t, test.OrderDetail{
					OrderId: 5,
					ItemId:  1,
				}, od5)
			},
			wantAffected: 2,
		},
		{
			name: "不同库不同表",
			values: []*test.OrderDetail{
				{
					// order_detail_db_1.order_detail_tab_0
					OrderId: 7,
					ItemId:  1,
				},
				{
					// order_detail_db_1.order_detail_tab_0
					OrderId: 9,
					ItemId:  1,
				},
				{
					OrderId: 4,
					ItemId:  2,
				},
			},
			after: func(t *testing.T) {
				od3 := s.findTgt(t, 7)
				assert.Equal(t, test.OrderDetail{
					OrderId: 7,
					ItemId:  1,
				}, od3)
				od5 := s.findTgt(t, 9)
				assert.Equal(t, test.OrderDetail{
					OrderId: 9,
					ItemId:  1,
				}, od5)
				od4 := s.findTgt(t, 4)
				assert.Equal(t, test.OrderDetail{
					OrderId: 4,
					ItemId:  2,
				}, od4)
			},
			wantAffected: 3,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := eorm.NewShardingInsert[test.OrderDetail](s.shardingDB).
				Values(tc.values).Exec(context.Background())
			affected, err := res.RowsAffected()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantAffected, affected)
			tc.after(t)
		})
	}
}

func (s *ShardingInsertTestSuite) findTgt(t *testing.T, orderID int64) test.OrderDetail {
	dbName := fmt.Sprintf(s.DBPattern, orderID%2)
	tabName := fmt.Sprintf(s.TablePattern, orderID%3)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ds, _ := s.dataSources["root:root@tcp(localhost:13307).0"]
	ctx = masterslave.UseMaster(ctx)
	rows, err := ds.Query(ctx, eorm.Query{
		DB:         "order_detail_db_1",
		Datasource: "root:root@tcp(localhost:13307).0",
		SQL:        fmt.Sprintf("SELECT `order_id`, `item_id` FROM `%s`.`%s` WHERE order_id=%d", dbName, tabName, orderID),
	})
	require.NoError(t, err)
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("no data")
	}
	var od test.OrderDetail
	err = rows.Scan(&od.OrderId, &od.ItemId)
	require.NoError(t, err)
	return od
}

func TestMySQL8ShardingInsert(t *testing.T) {
	suite.Run(t, &ShardingInsertTestSuite{
		ShardingSuite: newDefaultShardingSuite(),
	})
}
