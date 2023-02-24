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

package eorm

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodehub/eorm/internal/errs"
	"github.com/ecodehub/eorm/internal/slaves"
	"github.com/ecodehub/eorm/internal/slaves/roundrobin"
	"github.com/ecodehub/eorm/internal/test"

	"github.com/ecodehub/eorm/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardingSelector_Build(t *testing.T) {
	r := model.NewMetaRegistry()
	_, err := r.Register(&Order{},
		model.WithShardingKey("UserId"),
		model.WithDBShardingFunc(func(skVal any) (string, error) {
			db := skVal.(int64) / 100
			return fmt.Sprintf("order_db_%d", db), nil
		}),
		model.WithTableShardingFunc(func(skVal any) (string, error) {
			tbl := skVal.(int64) % 10
			return fmt.Sprintf("order_tab_%d", tbl), nil
		}))
	require.NoError(t, err)

	masterSlaveDB := masterSlaveMemoryDB()
	testCases := []struct {
		name    string
		builder ShardingQueryBuilder
		qs      []*ShardingQuery
		wantErr error
	}{
		{
			name: "not dst",
			builder: func() ShardingQueryBuilder {
				shardingDB, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_1": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](shardingDB).Where(C("UserId").EQ(int64(534)))
				return s
			}(),
			wantErr: errs.ErrNotFoundTargetDB,
		},
		{
			name: "too complex operator",
			builder: func() ShardingQueryBuilder {
				shardingDB, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_1": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](shardingDB).Where(C("UserId").GT(int64(123)))
				return s
			}(),
			wantErr: errs.NewUnsupportedOperatorError(opGT.text),
		},
		{
			name: "miss sharding key err",
			builder: func() ShardingQueryBuilder {
				db, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
				})
				require.NoError(t, err)
				s := NewShardingSelector[Order](db).Where(C("UserId").EQ(int64(123)))
				return s
			}(),
			wantErr: errs.ErrMissingShardingKey,
		},
		{
			name: "only eq",
			builder: func() ShardingQueryBuilder {
				shardingDB, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](shardingDB).Where(C("UserId").EQ(int64(123)))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_3` WHERE `user_id`=?;",
					Args: []any{int64(123)},
					DB:   "order_db_1",
				},
			},
		},
		{
			name: "columns",
			builder: func() ShardingQueryBuilder {
				shardingDB, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](shardingDB).
					Select(Columns("Content", "OrderId")).Where(C("UserId").EQ(int64(123)))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `content`,`order_id` FROM `order_db_1`.`order_tab_3` WHERE `user_id`=?;",
					Args: []any{int64(123)},
					DB:   "order_db_1",
				},
			},
		},
		{
			name: "invalid columns",
			builder: func() ShardingQueryBuilder {
				shardingDB, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](shardingDB).
					Select(C("Invalid")).Where(C("UserId").EQ(int64(123)))
				return s
			}(),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name: "order by",
			builder: func() ShardingQueryBuilder {
				shardingDB, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123))).OrderBy(ASC("UserId"), DESC("OrderId"))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE `user_id`=? ORDER BY `user_id` ASC,`order_id` DESC;",
					Args: []any{int64(123)},
					DB:   "order_db_1",
				},
			},
		},
		{
			name: "group by",
			builder: func() ShardingQueryBuilder {
				shardingDB, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123))).GroupBy("UserId", "OrderId")
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE `user_id`=? GROUP BY `user_id`,`order_id`;",
					Args: []any{int64(123)},
					DB:   "order_db_1",
				},
			},
		},
		{
			name: "having",
			builder: func() ShardingQueryBuilder {
				shardingDB, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123))).GroupBy("OrderId").Having(C("OrderId").EQ(int64(18)))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE `user_id`=? GROUP BY `order_id` HAVING `order_id`=?;",
					Args: []any{int64(123), int64(18)},
					DB:   "order_db_1",
				},
			},
		},
		{
			name: "where and left",
			builder: func() ShardingQueryBuilder {
				shardingDB, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).And(C("UserId").EQ(int64(123))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE (`order_id`=?) AND (`user_id`=?);",
					Args: []any{int64(12), int64(123)},
					DB:   "order_db_1",
				},
			},
		},
		{
			name: "where and right",
			builder: func() ShardingQueryBuilder {
				shardingDB, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123)).And(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE (`user_id`=?) AND (`order_id`=?);",
					Args: []any{int64(123), int64(12)},
					DB:   "order_db_1",
				},
			},
		},
		{
			name: "where or",
			builder: func() ShardingQueryBuilder {
				shardingDB, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
					"order_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
						"order_tab_4": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123)).Or(C("UserId").EQ(int64(234))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args: []any{int64(123), int64(234)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_4` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args: []any{int64(123), int64(234)},
					DB:   "order_db_2",
				},
			},
		},
		{
			name: "where or left broadcast",
			builder: func() ShardingQueryBuilder {
				orm, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
					"order_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_1": true,
						"order_tab_3": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](orm).
					Select(C("OrderId"), C("Content")).
					Where(C("OrderId").EQ(int64(12)).Or(C("UserId").EQ(int64(123))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args: []any{int64(12), int64(123)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args: []any{int64(12), int64(123)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args: []any{int64(12), int64(123)},
					DB:   "order_db_2",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_3` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args: []any{int64(12), int64(123)},
					DB:   "order_db_2",
				},
			},
		},
		{
			name: "where or right broadcast",
			builder: func() ShardingQueryBuilder {
				orm, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
					"order_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_1": true,
						"order_tab_3": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](orm).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123)).Or(C("OrderId").EQ(int64(12))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args: []any{int64(123), int64(12)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args: []any{int64(123), int64(12)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args: []any{int64(123), int64(12)},
					DB:   "order_db_2",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_3` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args: []any{int64(123), int64(12)},
					DB:   "order_db_2",
				},
			},
		},
		{
			name: "where and-or",
			builder: func() ShardingQueryBuilder {
				orm, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
					"order_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_1": true,
						"order_tab_3": true,
						"order_tab_4": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](orm).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123)).And(C("OrderId").EQ(int64(12))).Or(C("UserId").EQ(int64(234))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args: []any{int64(123), int64(12), int64(234)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_4` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args: []any{int64(123), int64(12), int64(234)},
					DB:   "order_db_2",
				},
			},
		},
		{
			name: "where and-or broadcast",
			builder: func() ShardingQueryBuilder {
				orm, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
					"order_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_1": true,
						"order_tab_3": true,
						"order_tab_4": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](orm).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123)).And(C("OrderId").EQ(int64(12)).
						Or(C("UserId").EQ(int64(234)))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE (`user_id`=?) AND ((`order_id`=?) OR (`user_id`=?));",
					Args: []any{int64(123), int64(12), int64(234)},
					DB:   "order_db_1",
				},
			},
		},
		{
			name: "where or-and all",
			builder: func() ShardingQueryBuilder {
				orm, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
					"order_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_1": true,
						"order_tab_3": true,
						"order_tab_4": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](orm).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123)).Or(C("UserId").EQ(int64(181)).And(C("UserId").EQ(int64(234)))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args: []any{int64(123), int64(181), int64(234)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args: []any{int64(123), int64(181), int64(234)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_4` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args: []any{int64(123), int64(181), int64(234)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_1` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args: []any{int64(123), int64(181), int64(234)},
					DB:   "order_db_2",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_3` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args: []any{int64(123), int64(181), int64(234)},
					DB:   "order_db_2",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_4` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args: []any{int64(123), int64(181), int64(234)},
					DB:   "order_db_2",
				},
			},
		},
		{
			name: "where or-and",
			builder: func() ShardingQueryBuilder {
				orm, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
					"order_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_1": true,
						"order_tab_3": true,
						"order_tab_4": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](orm).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123)).Or(C("UserId").EQ(int64(234))).
						And(C("OrderId").EQ(int64(24))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args: []any{int64(123), int64(234), int64(24)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_4` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args: []any{int64(123), int64(234), int64(24)},
					DB:   "order_db_2",
				},
			},
		},
		{
			name: "where or-and broadcast",
			builder: func() ShardingQueryBuilder {
				orm, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
					"order_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
						"order_tab_4": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](orm).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123)).Or(C("OrderId").EQ(int64(12))).
						And(C("UserId").EQ(int64(234))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_4` WHERE ((`user_id`=?) OR (`order_id`=?)) AND (`user_id`=?);",
					Args: []any{int64(123), int64(12), int64(234)},
					DB:   "order_db_2",
				},
			},
		},
		{
			name: "where or-or",
			builder: func() ShardingQueryBuilder {
				orm, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
					"order_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
						"order_tab_4": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](orm).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123)).Or(C("UserId").EQ(int64(253))).
						Or(C("UserId").EQ(int64(234))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args: []any{int64(123), int64(253), int64(234)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_3` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args: []any{int64(123), int64(253), int64(234)},
					DB:   "order_db_2",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_4` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args: []any{int64(123), int64(253), int64(234)},
					DB:   "order_db_2",
				},
			},
		},
		{
			name: "where or-or broadcast",
			builder: func() ShardingQueryBuilder {
				orm, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
					"order_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
						"order_tab_4": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](orm).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123)).Or(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(int64(234))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args: []any{int64(123), int64(12), int64(234)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_4` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args: []any{int64(123), int64(12), int64(234)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_3` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args: []any{int64(123), int64(12), int64(234)},
					DB:   "order_db_2",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_4` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args: []any{int64(123), int64(12), int64(234)},
					DB:   "order_db_2",
				},
			},
		},
		{
			name: "where and-and",
			builder: func() ShardingQueryBuilder {
				orm, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](orm).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123)).And(C("OrderId").EQ(int64(12))).
						And(C("OrderId").EQ(int64(23))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE ((`user_id`=?) AND (`order_id`=?)) AND (`order_id`=?);",
					Args: []any{int64(123), int64(12), int64(23)},
					DB:   "order_db_1",
				},
			},
		},
		{
			name: "where and-or-and",
			builder: func() ShardingQueryBuilder {
				orm, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
					"order_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
						"order_tab_4": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](orm).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123)).And(C("OrderId").EQ(int64(12))).
						Or(C("UserId").EQ(int64(234)).And(C("OrderId").EQ(int64(18)))))
				return s
			}(),
			qs: []*ShardingQuery{
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_3` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args: []any{int64(123), int64(12), int64(234), int64(18)},
					DB:   "order_db_1",
				},
				{
					SQL:  "SELECT `order_id`,`content` FROM `order_db_2`.`order_tab_4` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args: []any{int64(123), int64(12), int64(234), int64(18)},
					DB:   "order_db_2",
				},
			},
		},
		{
			name: "and empty",
			builder: func() ShardingQueryBuilder {
				shardingDB, err := OpenShardingDB("sqlite3", map[string]*MasterSlavesDB{
					"order_db_1": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_tab_3": true,
						"order_tab_4": true,
					}))
				require.NoError(t, err)
				s := NewShardingSelector[Order](shardingDB).
					Select(C("OrderId"), C("Content")).
					Where(C("UserId").EQ(int64(123)).And(C("UserId").EQ(int64(124))))
				return s
			}(),
			qs: []*ShardingQuery{},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			qs, err := c.builder.Build()
			assert.Equal(t, c.wantErr, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, c.qs, qs)
		})
	}
}

func TestSardingSelector_Get(t *testing.T) {
	r := model.NewMetaRegistry()
	_, err := r.Register(&test.OrderDetail{},
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

	mockDB, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()

	newMockSlaveNameGet(roundrobin.NewSlaves(mockDB))
	masterSlaveDB, err := OpenMasterSlaveDB("mysql", mockDB,
		MasterSlaveWithSlaves(newMockSlaveNameGet(roundrobin.NewSlaves(mockDB))))
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
				shardingDB, err := OpenShardingDB("mysql", map[string]*MasterSlavesDB{},
					ShardingDBOptionWithMetaRegistry(r))
				require.NoError(t, err)
				builder := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("ItemId").EQ(12))
				return builder
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {},
			wantErr:   errs.ErrNotGenShardingQuery,
		},
		{
			name: "only result one query",
			s: func() *ShardingSelector[test.OrderDetail] {
				shardingDB, err := OpenShardingDB("mysql", map[string]*MasterSlavesDB{
					"order_detail_db_1": masterSlaveDB,
					"order_detail_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_detail_tab_3": true,
						"order_detail_tab_4": true,
					}))
				require.NoError(t, err)
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
				shardingDB, err := OpenShardingDB("mysql", map[string]*MasterSlavesDB{
					"order_detail_db_1": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_detail_tab_3": true,
					}))
				require.NoError(t, err)
				builder := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(123))
				return builder
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {

				rows := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).
					AddRow(123, 10, "LeBron", "James")
				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_3` WHERE `order_id`=? LIMIT ?;").
					WithArgs(123, 1).
					WillReturnRows(rows)
			},
			wantRes: &test.OrderDetail{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
		},
		{
			name: "found tab 2",
			s: func() *ShardingSelector[test.OrderDetail] {
				shardingDB, err := OpenShardingDB("mysql", map[string]*MasterSlavesDB{
					"order_detail_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_detail_tab_4": true,
					}))
				require.NoError(t, err)
				builder := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(234))
				return builder
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).
					AddRow(234, 12, "Kevin", "Durant")
				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_2`.`order_detail_tab_4` WHERE `order_id`=? LIMIT ?;").
					WithArgs(234, 1).
					WillReturnRows(rows)
			},
			wantRes: &test.OrderDetail{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
		},
		{
			name: "found tab and",
			s: func() *ShardingSelector[test.OrderDetail] {
				shardingDB, err := OpenShardingDB("mysql", map[string]*MasterSlavesDB{
					"order_detail_db_2": masterSlaveDB,
				}, ShardingDBOptionWithMetaRegistry(r),
					ShardingDBOptionWithTables(map[string]bool{
						"order_detail_tab_4": true,
					}))
				require.NoError(t, err)
				builder := NewShardingSelector[test.OrderDetail](shardingDB).
					Where(C("OrderId").EQ(234).And(C("ItemId").EQ(12)))
				return builder
			}(),
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).
					AddRow(234, 12, "Kevin", "Durant")
				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_2`.`order_detail_tab_4` WHERE (`order_id`=?) AND (`item_id`=?) LIMIT ?;").
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

//func TestSardingSelector_GetMulti(t *testing.T) {
//	r := model.NewMetaRegistry()
//	_, err := r.Register(&test.OrderDetail{},
//		model.WithShardingKey("OrderId"),
//		model.WithDBShardingFunc(func(skVal any) (string, error) {
//			db := skVal.(int) / 100
//			return fmt.Sprintf("order_detail_db_%d", db), nil
//		}),
//		model.WithTableShardingFunc(func(skVal any) (string, error) {
//			tbl := skVal.(int) % 10
//			return fmt.Sprintf("order_detail_tab_%d", tbl), nil
//		}))
//	require.NoError(t, err)
//
//	mockDB, mock, err := sqlmock.New(
//		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer func() { _ = mockDB.Close() }()
//
//	newMockSlaveNameGet(roundrobin.NewSlaves(mockDB))
//	masterSlaveDB, err := OpenMasterSlaveDB("mysql", mockDB,
//		MasterSlaveWithSlaves(newMockSlaveNameGet(roundrobin.NewSlaves(mockDB))))
//	require.NoError(t, err)
//	shardingDB, err := OpenShardingDB("mysql", map[string]*MasterSlavesDB{
//		"order_detail_db_1": masterSlaveDB,
//		"order_detail_db_2": masterSlaveDB,
//	}, ShardingDBOptionWithMetaRegistry(r),
//		ShardingDBOptionWithTables(map[string]bool{
//			"order_detail_tab_3": true,
//			"order_detail_tab_4": true,
//		}))
//	require.NoError(t, err)
//
//	testCases := []struct {
//		name      string
//		s         *ShardingSelector[test.OrderDetail]
//		mockOrder func(mock sqlmock.Sqlmock)
//		wantErr   error
//		wantRes   []*test.OrderDetail
//	}{
//		{
//			name: "found tab or",
//			s: func() *ShardingSelector[test.OrderDetail] {
//				builder := NewShardingSelector[test.OrderDetail](shardingDB).
//					Where(C("OrderId").EQ(123).Or(C("OrderId").EQ(234)))
//				return builder
//			}(),
//			mockOrder: func(mock sqlmock.Sqlmock) {
//				rows1 := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).
//					AddRow(123, 10, "LeBron", "James")
//				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_3` WHERE (`order_id`=?) OR (`order_id`=?);").
//					WithArgs(123, 234).
//					WillReturnRows(rows1)
//				rows2 := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).
//					AddRow(234, 12, "Kevin", "Durant")
//				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_3` WHERE (`order_id`=?) OR ((`item_id`=?) AND (`order_id`=?));").
//					WithArgs(123, 234).
//					WillReturnRows(rows2)
//			},
//			wantRes: []*test.OrderDetail{
//				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
//				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
//			},
//		},
//		{
//			name: "found tab or broadcast",
//			s: func() *ShardingSelector[test.OrderDetail] {
//				builder := NewShardingSelector[test.OrderDetail](shardingDB).
//					Where(C("OrderId").EQ(123).Or(C("ItemId").EQ(12)))
//				return builder
//			}(),
//			mockOrder: func(mock sqlmock.Sqlmock) {
//				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_4` WHERE (`order_id`=?) OR (`item_id`=?);").
//					WithArgs(123, 12).
//					WillReturnRows()
//				rows2 := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).
//					AddRow(123, 10, "LeBron", "James")
//				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_3` WHERE (`order_id`=?) OR (`order_id`=?);").
//					WithArgs(123, 12).
//					WillReturnRows(rows2)
//				rows3 := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).
//					AddRow(234, 12, "Kevin", "Durant")
//				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_2`.`order_detail_tab_4` WHERE (`order_id`=?) OR (`order_id`=?);").
//					WithArgs(123, 12).
//					WillReturnRows(rows3)
//				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_2`.`order_detail_tab_3` WHERE (`order_id`=?) OR (`item_id`=?);").
//					WithArgs(123, 12).WillReturnRows()
//			},
//			wantRes: []*test.OrderDetail{
//				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
//				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
//			},
//		},
//		{
//			name: "found tab or-and",
//			s: func() *ShardingSelector[test.OrderDetail] {
//				builder := NewShardingSelector[test.OrderDetail](shardingDB).
//					Where(C("OrderId").EQ(234).
//						Or(C("ItemId").EQ(10).And(C("OrderId").EQ(123))))
//				return builder
//			}(),
//			mockOrder: func(mock sqlmock.Sqlmock) {
//				rows1 := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).
//					AddRow(123, 10, "LeBron", "James")
//				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_2`.`order_detail_tab_4` WHERE (`order_id`=?) OR ((`item_id`=?) AND (`order_id`=?));").
//					WithArgs(234, 10, 123).
//					WillReturnRows(rows1)
//				rows2 := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).
//					AddRow(234, 12, "Kevin", "Durant")
//				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_3` WHERE (`order_id`=?) OR ((`item_id`=?) AND (`order_id`=?));").
//					WithArgs(234, 10, 123).
//					WillReturnRows(rows2)
//			},
//			wantRes: []*test.OrderDetail{
//				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
//				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
//			},
//		},
//		{
//			name: "found tab and-or",
//			s: func() *ShardingSelector[test.OrderDetail] {
//				builder := NewShardingSelector[test.OrderDetail](shardingDB).
//					Where(C("OrderId").EQ(234).
//						And(C("ItemId").EQ(12).Or(C("OrderId").EQ(123))))
//				return builder
//			}(),
//			mockOrder: func(mock sqlmock.Sqlmock) {
//				rows := mock.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).
//					AddRow(123, 10, "LeBron", "James")
//				mock.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_2`.`order_detail_tab_4` WHERE (`order_id`=?) AND ((`item_id`=?) OR (`order_id`=?));").
//					WithArgs(234, 12, 123).
//					WillReturnRows(rows)
//			},
//			wantRes: []*test.OrderDetail{
//				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
//			},
//		},
//	}
//
//	for _, tc := range testCases {
//		t.Run(tc.name, func(t *testing.T) {
//			tc.mockOrder(mock)
//			ctx := UseMaster(context.Background())
//			res, err := tc.s.GetMulti(ctx)
//			assert.Equal(t, tc.wantErr, err)
//			if err != nil {
//				return
//			}
//			assert.ElementsMatch(t, tc.wantRes, res)
//		})
//	}
//}

type Order struct {
	UserId  int64
	OrderId int64
	Content string
	Account float64
}

type mockSlaveNamegeter struct {
	slaves.Slaves
}

func newMockSlaveNameGet(geter slaves.Slaves) *mockSlaveNamegeter {
	return &mockSlaveNamegeter{
		Slaves: geter,
	}
}

func (s *mockSlaveNamegeter) Next(ctx context.Context) (slaves.Slave, error) {
	slave, err := s.Slaves.Next(ctx)
	if err != nil {
		return slave, err
	}
	return slave, err
}
