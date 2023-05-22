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

package eorm

import (
	"context"
	"fmt"
	"testing"

	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/cluster"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/datasource/shardingsource"
	"github.com/ecodeclub/eorm/internal/model"
	"github.com/ecodeclub/eorm/internal/sharding"
	"github.com/ecodeclub/eorm/internal/sharding/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardingUpdater_Build(t *testing.T) {
	r := model.NewMetaRegistry()
	dbBase, tableBase, dsBase := 2, 3, 2
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab_%d", "%d.db.cluster.company.com:3306"
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
			TablePattern: &hash.Pattern{Name: tablePattern, Base: tableBase},
			DsPattern:    &hash.Pattern{Name: dsPattern, Base: dsBase},
		}))
	require.NoError(t, err)
	m := map[string]*masterslave.MasterSlavesDB{
		"order_db_0": MasterSlavesMemoryDB(),
		"order_db_1": MasterSlavesMemoryDB(),
		"order_db_2": MasterSlavesMemoryDB(),
	}
	clusterDB := cluster.NewClusterDB(m)
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
		"1.db.cluster.company.com:3306": clusterDB,
	}
	shardingDB, err := OpenDS("sqlite3",
		shardingsource.NewShardingDataSource(ds), DBOptionWithMetaRegistry(r))
	require.NoError(t, err)
	testCases := []struct {
		name    string
		builder sharding.QueryBuilder
		wantQs  []sharding.Query
		wantErr error
	}{
		{
			name: "where eq",
			builder: NewShardingUpdater[Order](shardingDB).Update(&Order{
				UserId: 1, OrderId: 1, Content: "1", Account: 1.0,
			}).Where(C("UserId").EQ(1)),
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `order_id`=?,`content`=?,`account`=? WHERE `user_id`=?;", "`order_db_1`", "`order_tab_1`"),
					Args:       []any{int64(1), "1", 1.0, 1},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or",
			builder: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(Columns("Content", "Account")).Where(C("UserId").EQ(123).Or(C("UserId").EQ(234))),
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE (`user_id`=?) OR (`user_id`=?);", "`order_db_1`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 123, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE (`user_id`=?) OR (`user_id`=?);", "`order_db_0`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 123, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		//{
		//	name: "插入多个元素, 但是不同的元素会被分配到同一个库",
		//	builder: NewShardingInsert[OrderInsert](shardingDB).Values([]*OrderInsert{
		//		&OrderInsert{UserId: 1, OrderId: 1, Content: "1", Account: 1.0},
		//		&OrderInsert{UserId: 7, OrderId: 7, Content: "7", Account: 7.0},
		//	}),
		//	wantQs: []sharding.Query{
		//		{
		//			SQL:        fmt.Sprintf("INSERT INTO %s.%s(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?),(?,?,?,?);", "`order_db_1`", "`order_tab_1`"),
		//			Args:       []any{1, int64(1), "1", 1.0, 7, int64(7), "7", 7.0},
		//			DB:         "order_db_1",
		//			Datasource: "1.db.cluster.company.com:3306",
		//		},
		//	},
		//},
		//{
		//	name: "插入多个元素, 有不同的元素会被分配到同一个库表",
		//	builder: NewShardingInsert[OrderInsert](shardingDB).Values([]*OrderInsert{
		//		&OrderInsert{UserId: 1, OrderId: 1, Content: "1", Account: 1.0},
		//		&OrderInsert{UserId: 7, OrderId: 7, Content: "7", Account: 7.0},
		//		&OrderInsert{UserId: 2, OrderId: 2, Content: "2", Account: 2.0},
		//		&OrderInsert{UserId: 8, OrderId: 8, Content: "8", Account: 8.0},
		//		&OrderInsert{UserId: 3, OrderId: 3, Content: "3", Account: 3.0},
		//	}),
		//	wantQs: []sharding.Query{
		//
		//		{
		//			SQL:        fmt.Sprintf("INSERT INTO %s.%s(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?),(?,?,?,?);", "`order_db_0`", "`order_tab_2`"),
		//			Args:       []any{2, int64(2), "2", 2.0, 8, int64(8), "8", 8.0},
		//			DB:         "order_db_0",
		//			Datasource: "0.db.cluster.company.com:3306",
		//		},
		//		{
		//			SQL:        fmt.Sprintf("INSERT INTO %s.%s(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);INSERT INTO %s.%s(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?),(?,?,?,?);", "`order_db_1`", "`order_tab_0`", "`order_db_1`", "`order_tab_1`"),
		//			Args:       []any{3, int64(3), "3", 3.0, 1, int64(1), "1", 1.0, 7, int64(7), "7", 7.0},
		//			DB:         "order_db_1",
		//			Datasource: "1.db.cluster.company.com:3306",
		//		},
		//	},
		//},
		//{
		//	name: "插入时，插入的列没有包含分库分表的列",
		//	builder: NewShardingInsert[OrderInsert](shardingDB).Values([]*OrderInsert{
		//		&OrderInsert{OrderId: 1, Content: "1", Account: 1.0},
		//	}).Columns([]string{"OrderId", "Content", "Account"}),
		//	wantErr: errs.ErrInsertShardingKeyNotFound,
		//},
		//{
		//	name: "插入时,忽略主键，但主键为shardingKey报错",
		//	builder: NewShardingInsert[OrderInsert](shardingDB).Values([]*OrderInsert{
		//		&OrderInsert{OrderId: 1, Content: "1", Account: 1.0},
		//	}).IgnorePK(),
		//	wantErr: errs.ErrInsertShardingKeyNotFound,
		//},
		//{
		//	name:    "values中没有元素报错",
		//	builder: NewShardingInsert[OrderInsert](shardingDB),
		//	wantErr: errors.New("插入0行"),
		//},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			qs, err := tc.builder.Build(context.Background())
			require.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, tc.wantQs, qs)
		})
	}
}
