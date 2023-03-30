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
	"testing"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves/roundrobin"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	slaves2 "github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"

	"github.com/ecodeclub/eorm/internal/datasource/shardingsource"

	"github.com/ecodeclub/eorm/internal/datasource/cluster"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/sharding"
	"github.com/ecodeclub/eorm/internal/sharding/hash"
	"github.com/ecodeclub/eorm/internal/test"

	"github.com/ecodeclub/eorm/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardingSelector_shadow_Build(t *testing.T) {
	r := model.NewMetaRegistry()
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.ShadowHash{
			Hash: &hash.Hash{
				ShardingKey:  "UserId",
				DBPattern:    &hash.Pattern{Name: "order_db_%d", Base: 2},
				TablePattern: &hash.Pattern{Name: "order_tab_%d", Base: 3},
				DsPattern:    &hash.Pattern{Name: "0.db.cluster.company.com:3306", NotSharding: true},
			},
			Prefix: "shadow_",
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
	}
	shardingDB, err := OpenDS("sqlite3",
		shardingsource.NewShardingDataSource(ds), DBOptionWithMetaRegistry(r))
	require.NoError(t, err)

	testCases := []struct {
		name    string
		builder sharding.QueryBuilder
		qs      []sharding.Query
		wantErr error
	}{
		{
			name: "only eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("UserId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "only eq broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("OrderId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `shadow_order_db_0`.`shadow_order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `shadow_order_db_0`.`shadow_order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `shadow_order_db_0`.`shadow_order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `shadow_order_db_1`.`shadow_order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `shadow_order_db_1`.`shadow_order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "columns",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(Columns("Content", "OrderId")).Where(C("UserId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `content`,`order_id` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "invalid columns",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("Invalid")).Where(C("UserId").EQ(123))
				return s
			}(),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name: "order by",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).OrderBy(ASC("UserId"), DESC("OrderId"))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE `user_id`=? ORDER BY `user_id` ASC,`order_id` DESC;",
					Args:       []any{123},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "group by",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).GroupBy("UserId", "OrderId")
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE `user_id`=? GROUP BY `user_id`,`order_id`;",
					Args:       []any{123},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "having",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).GroupBy("OrderId").Having(C("OrderId").EQ(int64(18)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE `user_id`=? GROUP BY `order_id` HAVING `order_id`=?;",
					Args:       []any{123, int64(18)},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and left",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).And(C("UserId").EQ(123)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE (`order_id`=?) AND (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and right",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE (`user_id`=?) AND (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_0` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or left broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).Or(C("UserId").EQ(123)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or right broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12)).
						Or(C("UserId").EQ(234))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE (`user_id`=?) AND ((`order_id`=?) OR (`user_id`=?));",
					Args:       []any{123, int64(12), 234},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(181).And(C("UserId").EQ(234))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args:       []any{123, 181, 234},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(234)).
						And(C("OrderId").EQ(int64(24))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, int64(24)},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, int64(24)},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))).
						And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) AND (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(253)).
						Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_1` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).
						And(C("OrderId").EQ(int64(23))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, int64(12), int64(23)},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(234).And(C("OrderId").EQ(int64(18)))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_1`.`shadow_order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, int64(12), 234, int64(18)},
					DB:         "shadow_order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `shadow_order_db_0`.`shadow_order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, int64(12), 234, int64(18)},
					DB:         "shadow_order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "and empty",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("UserId").EQ(124)))
				return s
			}(),
			qs: []sharding.Query{},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			ctx := hash.CtxWithDBKey(context.Background())
			qs, err := c.builder.Build(hash.CtxWithTableKey(ctx))
			assert.Equal(t, c.wantErr, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, c.qs, qs)
		})
	}
}

func TestShardingSelector_onlyDataSource_Build(t *testing.T) {
	r := model.NewMetaRegistry()
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: "order_db", NotSharding: true},
			TablePattern: &hash.Pattern{Name: "order_tab", NotSharding: true},
			DsPattern:    &hash.Pattern{Name: "%d.db.cluster.company.com:3306", Base: 2},
		}))
	require.NoError(t, err)
	m := map[string]*masterslave.MasterSlavesDB{
		"order_db": MasterSlavesMemoryDB(),
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
		qs      []sharding.Query
		wantErr error
	}{
		{
			name: "only eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("UserId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "only eq broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("OrderId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "columns",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(Columns("Content", "OrderId")).Where(C("UserId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `content`,`order_id` FROM `order_db`.`order_tab` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "invalid columns",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("Invalid")).Where(C("UserId").EQ(123))
				return s
			}(),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name: "order by",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).OrderBy(ASC("UserId"), DESC("OrderId"))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE `user_id`=? ORDER BY `user_id` ASC,`order_id` DESC;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "group by",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).GroupBy("UserId", "OrderId")
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE `user_id`=? GROUP BY `user_id`,`order_id`;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "having",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).GroupBy("OrderId").Having(C("OrderId").EQ(int64(18)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE `user_id`=? GROUP BY `order_id` HAVING `order_id`=?;",
					Args:       []any{123, int64(18)},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and left",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).And(C("UserId").EQ(123)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`order_id`=?) AND (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and right",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) AND (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or left broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).Or(C("UserId").EQ(123)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or right broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12)).
						Or(C("UserId").EQ(234))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) AND ((`order_id`=?) OR (`user_id`=?));",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(181).And(C("UserId").EQ(234))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args:       []any{123, 181, 234},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(234)).
						And(C("OrderId").EQ(int64(24))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, int64(24)},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, int64(24)},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))).
						And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`order_id`=?)) AND (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(253)).
						Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).
						And(C("OrderId").EQ(int64(23))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, int64(12), int64(23)},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(234).And(C("OrderId").EQ(int64(18)))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, int64(12), 234, int64(18)},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, int64(12), 234, int64(18)},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "and empty",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("UserId").EQ(124)))
				return s
			}(),
			qs: []sharding.Query{},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			qs, err := c.builder.Build(context.Background())
			assert.Equal(t, c.wantErr, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, c.qs, qs)
		})
	}
}

func TestShardingSelector_onlyTable_Build(t *testing.T) {
	r := model.NewMetaRegistry()
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: "order_db", NotSharding: true},
			TablePattern: &hash.Pattern{Name: "order_tab_%d", Base: 3},
			DsPattern:    &hash.Pattern{Name: "0.db.cluster.company.com:3306", NotSharding: true},
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
	}
	shardingDB, err := OpenDS("sqlite3",
		shardingsource.NewShardingDataSource(ds), DBOptionWithMetaRegistry(r))
	require.NoError(t, err)

	testCases := []struct {
		name    string
		builder sharding.QueryBuilder
		qs      []sharding.Query
		wantErr error
	}{
		{
			name: "only eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("UserId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab_0` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "only eq broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("OrderId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "columns",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(Columns("Content", "OrderId")).Where(C("UserId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `content`,`order_id` FROM `order_db`.`order_tab_0` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "invalid columns",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("Invalid")).Where(C("UserId").EQ(123))
				return s
			}(),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name: "order by",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).OrderBy(ASC("UserId"), DESC("OrderId"))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE `user_id`=? ORDER BY `user_id` ASC,`order_id` DESC;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "group by",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).GroupBy("UserId", "OrderId")
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE `user_id`=? GROUP BY `user_id`,`order_id`;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "having",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).GroupBy("OrderId").Having(C("OrderId").EQ(int64(18)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE `user_id`=? GROUP BY `order_id` HAVING `order_id`=?;",
					Args:       []any{123, int64(18)},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and left",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).And(C("UserId").EQ(123)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`order_id`=?) AND (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and right",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) AND (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or left broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).Or(C("UserId").EQ(123)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or right broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12)).
						Or(C("UserId").EQ(234))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) AND ((`order_id`=?) OR (`user_id`=?));",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(181).And(C("UserId").EQ(234))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args:       []any{123, 181, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(181).And(C("UserId").EQ(234))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args:       []any{123, 181, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(234)).
						And(C("OrderId").EQ(int64(24))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, int64(24)},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))).
						And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) AND (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(253)).
						Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_1` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).
						And(C("OrderId").EQ(int64(23))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, int64(12), int64(23)},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(234).And(C("OrderId").EQ(int64(18)))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, int64(12), 234, int64(18)},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "and empty",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("UserId").EQ(124)))
				return s
			}(),
			qs: []sharding.Query{},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			qs, err := c.builder.Build(context.Background())
			assert.Equal(t, c.wantErr, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, c.qs, qs)
		})
	}
}

func TestShardingSelector_onlyDB_Build(t *testing.T) {
	r := model.NewMetaRegistry()
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: "order_db_%d", Base: 2},
			TablePattern: &hash.Pattern{Name: "order_tab", NotSharding: true},
			DsPattern:    &hash.Pattern{Name: "0.db.cluster.company.com:3306", NotSharding: true},
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
	}
	shardingDB, err := OpenDS("sqlite3",
		shardingsource.NewShardingDataSource(ds), DBOptionWithMetaRegistry(r))
	require.NoError(t, err)

	testCases := []struct {
		name    string
		builder sharding.QueryBuilder
		qs      []sharding.Query
		wantErr error
	}{
		{
			name: "only eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("UserId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "only eq broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("OrderId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},

		{
			name: "columns",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(Columns("Content", "OrderId")).Where(C("UserId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `content`,`order_id` FROM `order_db_1`.`order_tab` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "invalid columns",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("Invalid")).Where(C("UserId").EQ(123))
				return s
			}(),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name: "order by",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).OrderBy(ASC("UserId"), DESC("OrderId"))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE `user_id`=? ORDER BY `user_id` ASC,`order_id` DESC;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "group by",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).GroupBy("UserId", "OrderId")
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE `user_id`=? GROUP BY `user_id`,`order_id`;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "having",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).GroupBy("OrderId").Having(C("OrderId").EQ(int64(18)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE `user_id`=? GROUP BY `order_id` HAVING `order_id`=?;",
					Args:       []any{123, int64(18)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and left",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).And(C("UserId").EQ(123)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`order_id`=?) AND (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and right",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`user_id`=?) AND (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or left broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).Or(C("UserId").EQ(123)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or right broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12)).
						Or(C("UserId").EQ(234))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`user_id`=?) AND ((`order_id`=?) OR (`user_id`=?));",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(181).And(C("UserId").EQ(234))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args:       []any{123, 181, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(234)).
						And(C("OrderId").EQ(int64(24))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, int64(24)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, int64(24)},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))).
						And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE ((`user_id`=?) OR (`order_id`=?)) AND (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(253)).
						Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).
						And(C("OrderId").EQ(int64(23))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, int64(12), int64(23)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(234).And(C("OrderId").EQ(int64(18)))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, int64(12), 234, int64(18)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, int64(12), 234, int64(18)},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "and empty",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("UserId").EQ(124)))
				return s
			}(),
			qs: []sharding.Query{},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			qs, err := c.builder.Build(context.Background())
			assert.Equal(t, c.wantErr, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, c.qs, qs)
		})
	}
}

func TestShardingSelector_all_Build(t *testing.T) {
	r := model.NewMetaRegistry()
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: "order_db_%d", Base: 2},
			TablePattern: &hash.Pattern{Name: "order_tab_%d", Base: 3},
			DsPattern:    &hash.Pattern{Name: "%d.db.cluster.company.com:3306", Base: 2},
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
		qs      []sharding.Query
		wantErr error
	}{
		{
			name: "only eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("UserId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "only eq broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("OrderId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "columns",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(Columns("Content", "OrderId")).Where(C("UserId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `content`,`order_id` FROM `order_db_1`.`order_tab_0` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "invalid columns",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("Invalid")).Where(C("UserId").EQ(123))
				return s
			}(),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name: "order by",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).OrderBy(ASC("UserId"), DESC("OrderId"))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE `user_id`=? ORDER BY `user_id` ASC,`order_id` DESC;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "group by",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).GroupBy("UserId", "OrderId")
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE `user_id`=? GROUP BY `user_id`,`order_id`;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "having",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).GroupBy("OrderId").Having(C("OrderId").EQ(int64(18)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE `user_id`=? GROUP BY `order_id` HAVING `order_id`=?;",
					Args:       []any{123, int64(18)},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and left",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).And(C("UserId").EQ(123)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`order_id`=?) AND (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and right",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) AND (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or left broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).Or(C("UserId").EQ(123)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or right broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12)).
						Or(C("UserId").EQ(234))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) AND ((`order_id`=?) OR (`user_id`=?));",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(181).And(C("UserId").EQ(234))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args:       []any{123, 181, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(234)).
						And(C("OrderId").EQ(int64(24))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, int64(24)},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, int64(24)},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))).
						And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) AND (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(253)).
						Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).
						And(C("OrderId").EQ(int64(23))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, int64(12), int64(23)},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(234).And(C("OrderId").EQ(int64(18)))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, int64(12), 234, int64(18)},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, int64(12), 234, int64(18)},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "and empty",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("UserId").EQ(124)))
				return s
			}(),
			qs: []sharding.Query{},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			qs, err := c.builder.Build(context.Background())
			assert.Equal(t, c.wantErr, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, c.qs, qs)
		})
	}
}

func TestShardingSelector_Build(t *testing.T) {
	r := model.NewMetaRegistry()
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: "order_db_%d", Base: 2},
			TablePattern: &hash.Pattern{Name: "order_tab_%d", Base: 3},
			DsPattern:    &hash.Pattern{Name: "0.db.cluster.company.com:3306", NotSharding: true},
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
	}
	shardingDB, err := OpenDS("sqlite3",
		shardingsource.NewShardingDataSource(ds), DBOptionWithMetaRegistry(r))
	require.NoError(t, err)

	testCases := []struct {
		name    string
		builder sharding.QueryBuilder
		qs      []sharding.Query
		wantErr error
	}{
		{
			name: "too complex operator",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("UserId").GT(123))
				return s
			}(),
			wantErr: errs.NewUnsupportedOperatorError(opGT.text),
		},
		{
			name: "too complex expr",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(Avg("UserId").EQ([]int{1, 2, 3}))
				return s
			}(),
			wantErr: errs.ErrUnsupportedTooComplexQuery,
		},
		{
			name: "miss sharding key err",
			builder: func() sharding.QueryBuilder {
				reg := model.NewMetaRegistry()
				meta, err := reg.Register(&Order{},
					model.WithTableShardingAlgorithm(&hash.Hash{}))
				require.NoError(t, err)
				require.NotNil(t, meta.ShardingAlgorithm)
				db, err := OpenDS("sqlite3",
					shardingsource.NewShardingDataSource(map[string]datasource.DataSource{
						"0.db.cluster.company.com:3306": MasterSlavesMemoryDB(),
					}),
					DBOptionWithMetaRegistry(reg))
				require.NoError(t, err)
				s := NewShardingSelector[Order](db).Where(C("UserId").EQ(123))
				return s
			}(),
			wantErr: errs.ErrMissingShardingKey,
		},
		{
			name: "only eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("UserId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "only eq broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("OrderId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "columns",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(Columns("Content", "OrderId")).Where(C("UserId").EQ(123))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `content`,`order_id` FROM `order_db_1`.`order_tab_0` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "invalid columns",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("Invalid")).Where(C("UserId").EQ(123))
				return s
			}(),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name: "order by",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).OrderBy(ASC("UserId"), DESC("OrderId"))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE `user_id`=? ORDER BY `user_id` ASC,`order_id` DESC;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "group by",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).GroupBy("UserId", "OrderId")
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE `user_id`=? GROUP BY `user_id`,`order_id`;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "having",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123)).GroupBy("OrderId").Having(C("OrderId").EQ(int64(18)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE `user_id`=? GROUP BY `order_id` HAVING `order_id`=?;",
					Args:       []any{123, int64(18)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and left",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).And(C("UserId").EQ(123)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`order_id`=?) AND (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and right",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) AND (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or left broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).Or(C("UserId").EQ(123)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{int64(12), 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or right broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, int64(12)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12)).
						Or(C("UserId").EQ(234))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) AND ((`order_id`=?) OR (`user_id`=?));",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(181).And(C("UserId").EQ(234))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args:       []any{123, 181, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(234)).
						And(C("OrderId").EQ(int64(24))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, int64(24)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, int64(24)},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))).
						And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) AND (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("UserId").EQ(253)).
						Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or broadcast",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).
						And(C("OrderId").EQ(int64(23))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, int64(12), int64(23)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or-and",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(234).And(C("OrderId").EQ(int64(18)))))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, int64(12), 234, int64(18)},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, int64(12), 234, int64(18)},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "and empty",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(123).And(C("UserId").EQ(124)))
				return s
			}(),
			qs: []sharding.Query{},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			qs, err := c.builder.Build(context.Background())
			assert.Equal(t, c.wantErr, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, c.qs, qs)
		})
	}
}

func TestShardingSelector_Get(t *testing.T) {
	r := model.NewMetaRegistry()
	_, err := r.Register(&test.OrderDetail{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "OrderId",
			DBPattern:    &hash.Pattern{Name: "order_detail_db_%d", Base: 2},
			TablePattern: &hash.Pattern{Name: "order_detail_tab_%d", Base: 3},
			DsPattern:    &hash.Pattern{Name: "0.db.slave.company.com:3306", NotSharding: true},
		}))
	require.NoError(t, err)

	mockDB, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()

	rbSlaves, err := roundrobin.NewSlaves(mockDB)
	require.NoError(t, err)
	masterSlaveDB := masterslave.NewMasterSlavesDB(
		mockDB, masterslave.MasterSlavesWithSlaves(newMockSlaveNameGet(rbSlaves)))
	require.NoError(t, err)

	m := map[string]datasource.DataSource{
		"0.db.slave.company.com:3306": masterSlaveDB,
	}
	shardingDB, err := OpenDS("mysql",
		shardingsource.NewShardingDataSource(m), DBOptionWithMetaRegistry(r))
	require.NoError(t, err)

	testCases := []struct {
		name      string
		s         *ShardingSelector[test.OrderDetail]
		mockOrder func(mock sqlmock.Sqlmock)
		wantErr   error
		wantRes   *test.OrderDetail
	}{
		{
			name: "not gen sharding query",
			s: func() *ShardingSelector[test.OrderDetail] {
				builder := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(12).And(C("OrderId").EQ(14)))
				return builder
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {},
			wantErr:   errs.ErrNotGenShardingQuery,
		},
		{
			name: "only result one query",
			s: func() *ShardingSelector[test.OrderDetail] {
				builder := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(123).Or(C("ItemId").EQ(12)))
				return builder
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {},
			wantErr:   errs.ErrOnlyResultOneQuery,
		},
		{
			name: "found tab 1",
			s: func() *ShardingSelector[test.OrderDetail] {
				builder := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(123))
				return builder
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).
					AddRow(123, 10, "LeBron", "James")
				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE `order_id`=? LIMIT ?;").
					WithArgs(123, 1).
					WillReturnRows(rows)
			},
			wantRes: &test.OrderDetail{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
		},
		{
			name: "found tab 2",
			s: func() *ShardingSelector[test.OrderDetail] {
				builder := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(234))
				return builder
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).
					AddRow(234, 12, "Kevin", "Durant")
				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`=? LIMIT ?;").
					WithArgs(234, 1).
					WillReturnRows(rows)
			},
			wantRes: &test.OrderDetail{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
		},
		{
			name: "found tab and",
			s: func() *ShardingSelector[test.OrderDetail] {
				builder := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(234).And(C("ItemId").EQ(12)))
				return builder
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).
					AddRow(234, 12, "Kevin", "Durant")
				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE (`order_id`=?) AND (`item_id`=?) LIMIT ?;").
					WithArgs(234, 12, 1).
					WillReturnRows(rows)
			},
			wantRes: &test.OrderDetail{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.mockOrder(mock)
			res, err := tc.s.Get(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, res)
		})
	}
}

type Order struct {
	UserId  int
	OrderId int64
	Content string
	Account float64
}

type testSlaves struct {
	slaves2.Slaves
}

func newMockSlaveNameGet(s slaves2.Slaves) *testSlaves {
	return &testSlaves{
		Slaves: s,
	}
}

func (s *testSlaves) Next(ctx context.Context) (slaves2.Slave, error) {
	slave, err := s.Slaves.Next(ctx)
	if err != nil {
		return slave, err
	}
	return slave, err
}
