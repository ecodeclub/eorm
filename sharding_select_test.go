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
	"errors"
	"fmt"
	"testing"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves/roundrobin"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"

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
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r))
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
	dsBase := 2
	dbPattern, tablePattern, dsPattern := "order_db", "order_tab", "%d.db.cluster.company.com:3306"
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: dbPattern, NotSharding: true},
			TablePattern: &hash.Pattern{Name: tablePattern, NotSharding: true},
			DsPattern:    &hash.Pattern{Name: dsPattern, Base: dsBase},
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
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r))
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
			name: "where lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").LT(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<?;"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where lt eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").LTEQ(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<=?;"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where eq and lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").EQ(12).And(C("UserId").LT(133)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) AND (`user_id`<?);",
					Args:       []any{12, 133},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").GT(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>?;"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where gt eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").GTEQ(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>=?;"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where eq and gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").EQ(12).And(C("UserId").GT(133)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) AND (`user_id`>?);",
					Args:       []any{12, 133},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where eq and lt or gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(12).
						And(C("UserId").LT(133)).Or(C("UserId").GT(234)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ((`user_id`=?) AND (`user_id`<?)) OR (`user_id`>?);"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 133, 234},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where in",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in and eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101).And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id` IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` NOT IN (?,?,?);"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 35, 101},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in and eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id` NOT IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in or eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).Or(C("UserId").EQ(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`=?);"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in or gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).Or(C("UserId").GT(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`>?);"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GT(101)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>?);"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{101},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").LT(101)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`<?);"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{101},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (gt and lt)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GT(12).And(C("UserId").LT(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>?) AND (`user_id`<?));"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 531},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (gt eq and lt eq)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GTEQ(12).And(C("UserId").LTEQ(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>=?) AND (`user_id`<=?));"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 531},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (in or gt)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").In(12, 35, 101).Or(C("UserId").GT(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`>?));"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (in or eq)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").In(12, 35, 101).Or(C("UserId").EQ(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`=?));"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
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
	tableBase := 3
	dbPattern, tablePattern, dsPattern := "order_db", "order_tab_%d", "0.db.cluster.company.com:3306"
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: dbPattern, NotSharding: true},
			TablePattern: &hash.Pattern{Name: tablePattern, Base: tableBase},
			DsPattern:    &hash.Pattern{Name: dsPattern, NotSharding: true},
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
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r))
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
			name: "where lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").LT(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<?;"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where lt eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").LTEQ(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<=?;"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where eq and lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").EQ(12).And(C("UserId").LT(133)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) AND (`user_id`<?);",
					Args:       []any{12, 133},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").GT(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>?;"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where gt eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").GTEQ(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>=?;"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where eq and gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").EQ(12).And(C("UserId").GT(133)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) AND (`user_id`>?);",
					Args:       []any{12, 133},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where eq and lt or gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(12).
						And(C("UserId").LT(133)).Or(C("UserId").GT(234)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ((`user_id`=?) AND (`user_id`<?)) OR (`user_id`>?);"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 133, 234},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where in",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_2` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in and eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101).And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` NOT IN (?,?,?);"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 35, 101},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in and eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id` NOT IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in or eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).Or(C("UserId").EQ(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`=?);"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in or gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).Or(C("UserId").GT(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`>?);"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GT(101)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>?);"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{101},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").LT(101)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`<?);"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{101},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (gt and lt)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GT(12).And(C("UserId").LT(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>?) AND (`user_id`<?));"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 531},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (gt eq and lt eq)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GTEQ(12).And(C("UserId").LTEQ(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>=?) AND (`user_id`<=?));"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 531},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (in or gt)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").In(12, 35, 101).Or(C("UserId").GT(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`>?));"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (in or eq)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").In(12, 35, 101).Or(C("UserId").EQ(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`=?));"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
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
	dbBase := 2
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab", "0.db.cluster.company.com:3306"
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
			TablePattern: &hash.Pattern{Name: tablePattern, NotSharding: true},
			DsPattern:    &hash.Pattern{Name: dsPattern, NotSharding: true},
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
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r))
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
			name: "where lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").LT(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<?;"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{1},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where lt eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").LTEQ(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<=?;"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{1},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where eq and lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").EQ(12).And(C("UserId").LT(133)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id`=?) AND (`user_id`<?);",
					Args:       []any{12, 133},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").GT(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>?;"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{1},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where gt eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").GTEQ(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>=?;"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{1},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where eq and gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").EQ(12).And(C("UserId").GT(133)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id`=?) AND (`user_id`>?);",
					Args:       []any{12, 133},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where eq and lt or gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(12).
						And(C("UserId").LT(133)).Or(C("UserId").GT(234)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ((`user_id`=?) AND (`user_id`<?)) OR (`user_id`>?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 133, 234},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where in",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in and eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101).And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id` IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101).Or(C("UserId").EQ(531)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101).Or(C("UserId").GT(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` IN (?,?,?)) OR (`user_id`>?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` NOT IN (?,?,?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 35, 101},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in and eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id` NOT IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in or eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).Or(C("UserId").EQ(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`=?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in or gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).Or(C("UserId").GT(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`>?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GT(101)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{101},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").LT(101)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`<?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{101},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (gt and lt)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GT(12).And(C("UserId").LT(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>?) AND (`user_id`<?));"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (gt eq and lt eq)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GTEQ(12).And(C("UserId").LTEQ(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>=?) AND (`user_id`<=?));"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (in or gt)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").In(12, 35, 101).Or(C("UserId").GT(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`>?));"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (in or eq)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").In(12, 35, 101).Or(C("UserId").EQ(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`=?));"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
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
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r))
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
			name: "where lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").LT(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<?;"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{1},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where lt eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").LTEQ(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<=?;"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{1},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where eq and lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").EQ(12).And(C("UserId").LT(133)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) AND (`user_id`<?);",
					Args:       []any{12, 133},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").GT(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>?;"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{1},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where gt eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").GTEQ(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>=?;"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{1},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where eq and gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").EQ(12).And(C("UserId").GT(133)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) AND (`user_id`>?);",
					Args:       []any{12, 133},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where eq and lt or gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(12).
						And(C("UserId").LT(133)).Or(C("UserId").GT(234)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ((`user_id`=?) AND (`user_id`<?)) OR (`user_id`>?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 133, 234},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where in",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in and eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101).And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101).Or(C("UserId").EQ(531)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101).Or(C("UserId").GT(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` IN (?,?,?)) OR (`user_id`>?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 35, 101, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not in",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` NOT IN (?,?,?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 35, 101},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not in and eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id` NOT IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in or eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).Or(C("UserId").EQ(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`=?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 35, 101, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not in or gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).Or(C("UserId").GT(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`>?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 35, 101, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GT(101)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{101},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").LT(101)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`<?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{101},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not (gt and lt)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GT(12).And(C("UserId").LT(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>?) AND (`user_id`<?));"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not (gt eq and lt eq)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GTEQ(12).And(C("UserId").LTEQ(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>=?) AND (`user_id`<=?));"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not (in or gt)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").In(12, 35, 101).Or(C("UserId").GT(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`>?));"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 35, 101, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not (in or eq)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").In(12, 35, 101).Or(C("UserId").EQ(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`=?));"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 35, 101, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
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
	dbBase, tableBase := 2, 3
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab_%d", "0.db.cluster.company.com:3306"
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
			TablePattern: &hash.Pattern{Name: tablePattern, Base: tableBase},
			DsPattern:    &hash.Pattern{Name: dsPattern, NotSharding: true},
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
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r))
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
			name: "where and-or all",
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
			name: "where and-or",
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
			name: "where lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").LT(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{1},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where lt eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").LTEQ(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<=?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{1},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").GT(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{1},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where gt eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).Where(C("UserId").GTEQ(1))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>=?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{1},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where eq and lt or gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(12).
						And(C("UserId").LT(133)).Or(C("UserId").GT(234)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ((`user_id`=?) AND (`user_id`<?)) OR (`user_id`>?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 133, 234},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where in",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in and eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101).And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101).Or(C("UserId").EQ(531)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").In(12, 35, 101).Or(C("UserId").GT(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` IN (?,?,?)) OR (`user_id`>?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 35, 101, 531},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not in",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` NOT IN (?,?,?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 35, 101},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not in and eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).And(C("UserId").EQ(234)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id` NOT IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in or eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).Or(C("UserId").EQ(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 35, 101, 531},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not in or gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").NotIn(12, 35, 101).Or(C("UserId").GT(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`>?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 35, 101, 531},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not gt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GT(101)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{101},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not lt",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").LT(101)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`<?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{101},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not gt eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GTEQ(101)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{101},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not lt eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").LTEQ(101)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`<=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{101},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not eq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").EQ(101)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{101},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not neq",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").NEQ(101)))
				return s
			}(),
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE NOT (`user_id`!=?);",
					Args:       []any{101},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not (gt and lt)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GT(12).And(C("UserId").LT(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>?) AND (`user_id`<?));"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 531},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not (gt eq and lt eq)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").GTEQ(12).And(C("UserId").LTEQ(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>=?) AND (`user_id`<=?));"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 531},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not (in or gt)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").In(12, 35, 101).Or(C("UserId").GT(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`>?));"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 35, 101, 531},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not (in or eq)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").In(12, 35, 101).Or(C("UserId").EQ(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`=?));"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 35, 101, 531},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not (eq and eq)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").EQ(12).And(C("UserId").EQ(531))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`=?) AND (`user_id`=?));"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 531},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not (eq and eq not sharding key)",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(Not(C("UserId").EQ(12).And(C("OrderId").EQ(111))))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`=?) AND (`order_id`=?));"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 111},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where between",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").GTEQ(12).And(C("UserId").LTEQ(531)))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id`>=?) AND (`user_id`<=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 531},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "not where",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content"))
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s`;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "select from",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).From(&Order{})
				return s
			}(),
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s`;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
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

func TestShardingSelector_Build_Error(t *testing.T) {
	r := model.NewMetaRegistry()
	dbBase, tableBase := 2, 3
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab_%d", "0.db.cluster.company.com:3306"
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
			TablePattern: &hash.Pattern{Name: tablePattern, Base: tableBase},
			DsPattern:    &hash.Pattern{Name: dsPattern, NotSharding: true},
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
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r))
	require.NoError(t, err)

	testCases := []struct {
		name    string
		builder sharding.QueryBuilder
		qs      []sharding.Query
		wantErr error
	}{
		{
			name: "not and left too complex operator",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(Not(C("Content").
					Like("%kfc").And(C("OrderId").EQ(101))))
				return s
			}(),
			wantErr: errs.NewUnsupportedOperatorError(opLike.Text),
		},
		{
			name: "not or left too complex operator",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(Not(C("Content").
					Like("%kfc").Or(C("OrderId").EQ(101))))
				return s
			}(),
			wantErr: errs.NewUnsupportedOperatorError(opLike.Text),
		},
		{
			name: "not and right too complex operator",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(Not(C("OrderId").
					EQ(101).And(C("Content").Like("%kfc"))))
				return s
			}(),
			wantErr: errs.NewUnsupportedOperatorError(opLike.Text),
		},
		{
			name: "not or right too complex operator",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(Not(C("OrderId").
					EQ(101).Or(C("Content").Like("%kfc"))))
				return s
			}(),
			wantErr: errs.NewUnsupportedOperatorError(opLike.Text),
		},
		{
			name: "invalid field err",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("ccc"))
				return s
			}(),
			wantErr: errs.NewInvalidFieldError("ccc"),
		},
		{
			name: "group by invalid field err",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("UserId")).GroupBy("ccc")
				return s
			}(),
			wantErr: errs.NewInvalidFieldError("ccc"),
		},
		{
			name: "order by invalid field err",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Select(C("UserId")).OrderBy(ASC("ccc"))
				return s
			}(),
			wantErr: errs.NewInvalidFieldError("ccc"),
		},
		{
			name: "pointer only err",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[int64](shardingDB)
				return s
			}(),
			wantErr: errs.ErrPointerOnly,
		},
		{
			name: "too complex operator",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(C("Content").Like("%kfc"))
				return s
			}(),
			wantErr: errs.NewUnsupportedOperatorError(opLike.Text),
		},
		{
			name: "too complex expr",
			builder: func() sharding.QueryBuilder {
				s := NewShardingSelector[Order](shardingDB).Where(Avg("UserId").EQ(1))
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
					DBWithMetaRegistry(reg))
				require.NoError(t, err)
				s := NewShardingSelector[Order](db).Where(C("UserId").EQ(123))
				return s
			}(),
			wantErr: errs.ErrMissingShardingKey,
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			_, err = c.builder.Build(context.Background())
			assert.Equal(t, c.wantErr, err)
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
		shardingsource.NewShardingDataSource(m), DBWithMetaRegistry(r))
	require.NoError(t, err)

	testCases := []struct {
		name      string
		s         *ShardingSelector[test.OrderDetail]
		mockOrder func(mock sqlmock.Sqlmock)
		wantErr   error
		wantRes   *test.OrderDetail
	}{
		{
			name: "invalid field err",
			s: func() *ShardingSelector[test.OrderDetail] {
				b := NewShardingSelector[test.OrderDetail](shardingDB).Select(C("ccc"))
				return b
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {},
			wantErr:   errs.NewInvalidFieldError("ccc"),
		},
		{
			name: "not gen sharding query",
			s: func() *ShardingSelector[test.OrderDetail] {
				b := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(12).And(C("OrderId").EQ(14)))
				return b
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {},
			wantErr:   errs.ErrNotGenShardingQuery,
		},
		{
			name: "no rows err",
			s: func() *ShardingSelector[test.OrderDetail] {
				b := NewShardingSelector[test.OrderDetail](shardingDB).Select(C("UsingCol1")).
					Where(C("OrderId").EQ(123))
				return b
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				mock.ExpectQuery("SELECT `using_col1` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE `order_id`=? LIMIT ?;").
					WithArgs(123, 1).WillReturnRows(rows)
			},
			wantErr: ErrNoRows,
		},
		{
			name: "query err",
			s: func() *ShardingSelector[test.OrderDetail] {
				b := NewShardingSelector[test.OrderDetail](shardingDB).Select(C("UsingCol1")).
					Where(C("OrderId").EQ(123))
				return b
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT `using_col1` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE `order_id`=? LIMIT ?;").
					WithArgs(123, 1).WillReturnError(errors.New("query exception"))
			},
			wantErr: errors.New("query exception"),
		},
		{
			name: "multi row err",
			s: func() *ShardingSelector[test.OrderDetail] {
				b := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(123))
				return b
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2", "using_col3"}).
					AddRow(123, 10, "LeBron", "James", "de")
				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE `order_id`=? LIMIT ?;").
					WithArgs(123, 1).WillReturnRows(rows)
			},
			wantErr: errs.ErrTooManyColumns,
		},
		{
			name: "only result one query",
			s: func() *ShardingSelector[test.OrderDetail] {
				b := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(123).Or(C("ItemId").EQ(12)))
				return b
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {},
			wantErr:   errs.ErrOnlyResultOneQuery,
		},
		{
			name: "found tab 1",
			s: func() *ShardingSelector[test.OrderDetail] {
				b := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(123))
				return b
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
				b := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(234))
				return b
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
				b := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(234).And(C("ItemId").EQ(12)))
				return b
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

func TestShardingSelector_GetMulti(t *testing.T) {
	r := model.NewMetaRegistry()
	_, err := r.Register(&test.OrderDetail{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "OrderId",
			DBPattern:    &hash.Pattern{Name: "order_detail_db_%d", Base: 2},
			TablePattern: &hash.Pattern{Name: "order_detail_tab_%d", Base: 3},
			DsPattern:    &hash.Pattern{Name: "0.db.cluster.company.com:3306", NotSharding: true},
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

	mockDB2, mock2, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB2.Close() }()

	rbSlaves2, err := roundrobin.NewSlaves(mockDB2)
	require.NoError(t, err)
	masterSlaveDB2 := masterslave.NewMasterSlavesDB(
		mockDB2, masterslave.MasterSlavesWithSlaves(newMockSlaveNameGet(rbSlaves2)))
	require.NoError(t, err)

	clusterDB := cluster.NewClusterDB(map[string]*masterslave.MasterSlavesDB{
		"order_detail_db_0": masterSlaveDB,
		"order_detail_db_1": masterSlaveDB2,
	})
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
	}
	shardingDB, err := OpenDS("mysql",
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r))
	require.NoError(t, err)

	testCases := []struct {
		name      string
		s         *ShardingSelector[test.OrderDetail]
		mockOrder func(mock1, mock2 sqlmock.Sqlmock)
		wantErr   error
		wantRes   []*test.OrderDetail
	}{
		{
			name: "invalid field err",
			s: func() *ShardingSelector[test.OrderDetail] {
				b := NewShardingSelector[test.OrderDetail](shardingDB).Select(C("ccc"))
				return b
			}(),
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {},
			wantErr:   errs.NewInvalidFieldError("ccc"),
		},
		{
			name: "multi row err",
			s: func() *ShardingSelector[test.OrderDetail] {
				b := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(123))
				return b
			}(),
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				rows := mock2.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2", "using_col3"}).
					AddRow(123, 10, "LeBron", "James", "de")
				mock2.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE `order_id`=?;").
					WithArgs(123).WillReturnRows(rows)
			},
			wantErr: errs.ErrTooManyColumns,
		},
		{
			name: "found tab or",
			s: func() *ShardingSelector[test.OrderDetail] {
				b := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(123).Or(C("OrderId").EQ(234)))
				return b
			}(),
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				rows1 := mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				rows1.AddRow(234, 12, "Kevin", "Durant")
				mock1.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);").
					WithArgs(123, 234).WillReturnRows(rows1)
				rows2 := mock2.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				rows2.AddRow(123, 10, "LeBron", "James")
				mock2.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);").
					WithArgs(123, 234).WillReturnRows(rows2)
			},
			wantRes: []*test.OrderDetail{
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			},
		},
		{
			name: "err merge rows diff",
			s: func() *ShardingSelector[test.OrderDetail] {
				b := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(123).Or(C("OrderId").EQ(234)))
				return b
			}(),
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				rows1 := mock1.NewRows([]string{"order_id", "ite_id", "using_col1", "using_col2"})
				rows1.AddRow(234, 12, "Kevin", "Durant")
				mock1.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);").
					WithArgs(123, 234).WillReturnRows(rows1)
				rows2 := mock2.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				rows2.AddRow(123, 10, "LeBron", "James")
				mock2.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);").
					WithArgs(123, 234).WillReturnRows(rows2)
			},
			wantErr: errors.New("merger: sql.Rows列表中的字段不同"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.mockOrder(mock, mock2)
			res, err := tc.s.GetMulti(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, tc.wantRes, res)
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
	slaves.Slaves
}

func newMockSlaveNameGet(s slaves.Slaves) *testSlaves {
	return &testSlaves{
		Slaves: s,
	}
}

func (s *testSlaves) Next(ctx context.Context) (slaves.Slave, error) {
	slave, err := s.Slaves.Next(ctx)
	if err != nil {
		return slave, err
	}
	return slave, err
}
