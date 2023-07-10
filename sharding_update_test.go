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
	"database/sql"
	"fmt"
	"regexp"
	"testing"

	"go.uber.org/multierr"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/cluster"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/datasource/shardingsource"
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/model"
	"github.com/ecodeclub/eorm/internal/sharding"
	"github.com/ecodeclub/eorm/internal/sharding/hash"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestShardingUpdater_Build(t *testing.T) {
	r := model.NewMetaRegistry()
	dbBase, tableBase := 2, 3
	orderDBPattern, orderTablePattern := "order_db_%d", "order_tab_%d"
	dsPattern := "0.db.cluster.company.com:3306"
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: orderDBPattern, Base: dbBase},
			TablePattern: &hash.Pattern{Name: orderTablePattern, Base: tableBase},
			DsPattern:    &hash.Pattern{Name: dsPattern, NotSharding: true},
		}))
	require.NoError(t, err)
	r2 := model.NewMetaRegistry()
	_, err = r2.Register(&OrderDetail{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "OrderId",
			DBPattern:    &hash.Pattern{Name: "order_detail_db_%d", Base: dbBase},
			TablePattern: &hash.Pattern{Name: "order_detail_tab_%d", Base: tableBase},
			DsPattern:    &hash.Pattern{Name: dsPattern, NotSharding: true},
		}))
	require.NoError(t, err)
	m := map[string]*masterslave.MasterSlavesDB{
		"order_db_0":        MasterSlavesMemoryDB(),
		"order_db_1":        MasterSlavesMemoryDB(),
		"order_db_2":        MasterSlavesMemoryDB(),
		"order_detail_db_0": MasterSlavesMemoryDB(),
		"order_detail_db_1": MasterSlavesMemoryDB(),
		"order_detail_db_2": MasterSlavesMemoryDB(),
	}
	clusterDB := cluster.NewClusterDB(m)
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
	}
	shardingDB, err := OpenDS("sqlite3",
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r))
	require.NoError(t, err)
	shardingDB2, err := OpenDS("sqlite3",
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r2))
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
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "not where",
			builder: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(C("Content"), C("Account")),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where eq ignore zero val",
			builder: NewShardingUpdater[OrderDetail](shardingDB2).Update(&OrderDetail{
				UsingCol1: "Jack", UsingCol2: &sql.NullString{String: "Jerry", Valid: true},
			}).SkipZeroValue().Where(C("OrderId").EQ(1)),
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `using_col1`=?,`using_col2`=? WHERE `order_id`=?;", "`order_detail_db_1`", "`order_detail_tab_1`"),
					Args:       []any{"Jack", &sql.NullString{String: "Jerry", Valid: true}, 1},
					DB:         "order_detail_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where eq ignore nil val",
			builder: NewShardingUpdater[OrderDetail](shardingDB2).Update(&OrderDetail{
				UsingCol1: "Jack", ItemId: 11,
			}).SkipNilValue().Where(C("OrderId").EQ(1)),
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `item_id`=?,`using_col1`=? WHERE `order_id`=?;", "`order_detail_db_1`", "`order_detail_tab_1`"),
					Args:       []any{11, "Jack", 1},
					DB:         "order_detail_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or",
			builder: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(Columns("Content", "Account")).
				Where(C("UserId").EQ(123).Or(C("UserId").EQ(234))),
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE (`user_id`=?) OR (`user_id`=?);", "`order_db_1`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 123, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE (`user_id`=?) OR (`user_id`=?);", "`order_db_0`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 123, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or broadcast",
			builder: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(Columns("Content", "Account")).
				Where(C("UserId").EQ(123).Or(C("OrderId").EQ(int64(2)))),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE (`user_id`=?) OR (`order_id`=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 123, int64(2)},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where and empty",
			builder: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(Columns("Content", "Account")).
				Where(C("UserId").EQ(123).And(C("UserId").EQ(234))),
			wantQs: []sharding.Query{},
		},
		{
			name: "where and or",
			builder: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(Columns("Content", "Account")).
				Where(C("UserId").EQ(123).And(C("OrderId").EQ(int64(12))).
					Or(C("UserId").EQ(234))),
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);", "`order_db_1`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 123, int64(12), 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);", "`order_db_0`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 123, int64(12), 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			builder: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(Columns("Content", "Account")).
				Where(C("UserId").EQ(123).
					Or(C("UserId").EQ(181).And(C("UserId").EQ(234)))),
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));", "`order_db_1`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 123, 181, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where lt",
			builder: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(Columns("Content", "Account")).
				Where(C("UserId").LT(123)),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE `user_id`<?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 123},
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
			builder: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(Columns("Content", "Account")).
				Where(C("UserId").LTEQ(123)),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE `user_id`<=?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 123},
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
			builder: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(Columns("Content", "Account")).Where(C("UserId").GT(123)),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE `user_id`>?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 123},
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
			builder: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(Columns("Content", "Account")).Where(C("UserId").GTEQ(123)),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE `user_id`>=?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 123},
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
			builder: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(Columns("Content", "Account")).
				Where(C("UserId").EQ(12).And(C("UserId").
					LT(133)).Or(C("UserId").GT(234))),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE ((`user_id`=?) AND (`user_id`<?)) OR (`user_id`>?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 12, 133, 234},
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
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(C("UserId").In(12, 35, 101)),
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE `user_id` IN (?,?,?);", "`order_db_1`", "`order_tab_2`"),
					Args:       []any{"1", 1.0, 12, 35, 101},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE `user_id` IN (?,?,?);", "`order_db_0`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 12, 35, 101},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in and eq",
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(C("UserId").In(12, 35, 101).And(C("UserId").EQ(234))),
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE (`user_id` IN (?,?,?)) AND (`user_id`=?);", "`order_db_0`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 12, 35, 101, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or eq",
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(C("UserId").In(12, 35, 101).Or(C("UserId").EQ(531))),
			wantQs: []sharding.Query{
				{
					SQL:        "UPDATE `order_db_1`.`order_tab_2` SET `content`=?,`account`=? WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{"1", 1.0, 12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "UPDATE `order_db_1`.`order_tab_0` SET `content`=?,`account`=? WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{"1", 1.0, 12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "UPDATE `order_db_0`.`order_tab_0` SET `content`=?,`account`=? WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{"1", 1.0, 12, 35, 101, 531},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in",
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(C("UserId").NotIn(12, 35, 101)),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE `user_id` NOT IN (?,?,?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 12, 35, 101},
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
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(Not(C("UserId").GT(101))),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE NOT (`user_id`>?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 101},
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
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(Not(C("UserId").LT(101))),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE NOT (`user_id`<?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 101},
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
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(Not(C("UserId").GTEQ(101))),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE NOT (`user_id`>=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 101},
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
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(Not(C("UserId").LTEQ(101))),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE NOT (`user_id`<=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 101},
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
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(Not(C("UserId").EQ(101))),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE NOT (`user_id`=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 101},
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
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(Not(C("UserId").NEQ(101))),
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE NOT (`user_id`!=?);", "`order_db_1`", "`order_tab_2`"),
					Args:       []any{"1", 1.0, 101},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where between",
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(C("UserId").GTEQ(12).And(C("UserId").LTEQ(531))),
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE (`user_id`>=?) AND (`user_id`<=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 12, 531},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
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

func TestShardingUpdater_Build_Error(t *testing.T) {
	r := model.NewMetaRegistry()
	dbBase, tableBase, dsBase := 2, 3, 2
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab_%d", "0.db.cluster.company.com:3306"
	_, err := r.Register(&Order{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
			TablePattern: &hash.Pattern{Name: tablePattern, Base: tableBase},
			DsPattern:    &hash.Pattern{Name: dsPattern, Base: dsBase, NotSharding: true},
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
		wantQs  []sharding.Query
		wantErr error
	}{
		{
			name: "err update sharding key unsupported Columns",
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{UserId: 12, Content: "1", Account: 1.0}).
				Set(Columns("UserId", "Content", "Account")),
			wantErr: errs.NewErrUpdateShardingKeyUnsupported("UserId"),
		},
		{
			name: "err update sharding key unsupported Column",
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{UserId: 12, Content: "1", Account: 1.0}).
				Set(C("UserId"), C("Content"), C("Account")),
			wantErr: errs.NewErrUpdateShardingKeyUnsupported("UserId"),
		},
		{
			name: "not or left too complex operator",
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(Not(C("Content").Like("%kfc").Or(C("OrderId").EQ(101)))),
			wantErr: errs.NewUnsupportedOperatorError(opLike.Text),
		},
		{
			name: "not and right too complex operator",
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(Not(C("OrderId").EQ(101).And(C("Content").Like("%kfc")))),
			wantErr: errs.NewUnsupportedOperatorError(opLike.Text),
		},
		{
			name: "not or right too complex operator",
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).
				Where(Not(C("OrderId").EQ(101).Or(C("Content").Like("%kfc")))),
			wantErr: errs.NewUnsupportedOperatorError(opLike.Text),
		},
		{
			name: "invalid field err",
			builder: NewShardingUpdater[Order](shardingDB).
				Set(Columns("Content", "ccc")),
			wantErr: errs.NewInvalidFieldError("ccc"),
		},
		{
			name: "pointer only err",
			builder: NewShardingUpdater[int64](shardingDB).
				Set(Columns("Content", "Account")).
				Where(Not(C("OrderId").EQ(101).And(C("Content").Like("%kfc")))),
			wantErr: errs.ErrPointerOnly,
		},
		{
			name: "too complex operator",
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).Where(C("Content").Like("%kfc")),
			wantErr: errs.NewUnsupportedOperatorError(opLike.Text),
		},
		{
			name: "too complex expr",
			builder: NewShardingUpdater[Order](shardingDB).
				Update(&Order{Content: "1", Account: 1.0}).
				Set(Columns("Content", "Account")).Where(Avg("UserId").EQ(1)),
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
				s := NewShardingUpdater[Order](db).
					Update(&Order{Content: "1", Account: 1.0}).
					Set(Columns("Content", "Account")).Where(C("UserId").EQ(123))
				return s
			}(),
			wantErr: errs.ErrMissingShardingKey,
		},
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

type ShardingUpdaterSuite struct {
	suite.Suite
	mock01   sqlmock.Sqlmock
	mockDB01 *sql.DB
	mock02   sqlmock.Sqlmock
	mockDB02 *sql.DB
}

func (s *ShardingUpdaterSuite) SetupSuite() {
	t := s.T()
	var err error
	s.mockDB01, s.mock01, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockDB02, s.mock02, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

}

func (s *ShardingUpdaterSuite) TearDownTest() {
	_ = s.mockDB01.Close()
	_ = s.mockDB02.Close()
}

func (s *ShardingUpdaterSuite) TestShardingUpdater_Exec() {
	t := s.T()
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
		"order_db_0": MasterSlavesMockDB(s.mockDB01),
		"order_db_1": MasterSlavesMockDB(s.mockDB02),
	}
	clusterDB := cluster.NewClusterDB(m)
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
	}
	shardingDB, err := OpenDS("sqlite3",
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r))
	require.NoError(t, err)
	testCases := []struct {
		name             string
		exec             sharding.Executor
		mockDB           func()
		wantAffectedRows int64
		wantErr          error
	}{
		{
			name: "invalid field err",
			exec: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(Columns("Content", "ccc")).Where(C("UserId").EQ(1)),
			mockDB:  func() {},
			wantErr: multierr.Combine(errs.NewInvalidFieldError("ccc")),
		},
		{
			name: "update fail",
			exec: NewShardingUpdater[Order](shardingDB).Update(&Order{
				UserId: 1, OrderId: 1, Content: "1", Account: 1.0,
			}).Where(C("UserId").EQ(1)),
			mockDB: func() {
				s.mock02.ExpectExec(regexp.QuoteMeta("UPDATE `order_db_1`.`order_tab_1` SET `order_id`=?,`content`=?,`account`=? WHERE `user_id`=?;")).
					WithArgs(int64(1), "1", 1.0, 1).WillReturnError(newMockErr("db"))
			},
			wantErr: multierr.Combine(newMockErr("db")),
		},
		{
			name: "where eq",
			exec: NewShardingUpdater[Order](shardingDB).Update(&Order{
				UserId: 1, OrderId: 1, Content: "1", Account: 1.0,
			}).Where(C("UserId").EQ(1)),
			mockDB: func() {
				s.mock02.ExpectExec(regexp.QuoteMeta("UPDATE `order_db_1`.`order_tab_1` SET `order_id`=?,`content`=?,`account`=? WHERE `user_id`=?;")).
					WithArgs(int64(1), "1", 1.0, 1).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			wantAffectedRows: 1,
		},
		{
			name: "where or",
			exec: NewShardingUpdater[Order](shardingDB).Update(&Order{
				Content: "1", Account: 1.0,
			}).Set(Columns("Content", "Account")).
				Where(C("UserId").EQ(123).Or(C("UserId").EQ(234))),
			mockDB: func() {
				s.mock02.ExpectExec(regexp.QuoteMeta("UPDATE `order_db_1`.`order_tab_0` SET `content`=?,`account`=? WHERE (`user_id`=?) OR (`user_id`=?);")).
					WithArgs("1", 1.0, 123, 234).WillReturnResult(sqlmock.NewResult(1, 2))
				s.mock01.ExpectExec(regexp.QuoteMeta("UPDATE `order_db_0`.`order_tab_0` SET `content`=?,`account`=? WHERE (`user_id`=?) OR (`user_id`=?);")).
					WithArgs("1", 1.0, 123, 234).WillReturnResult(sqlmock.NewResult(1, 2))
			},
			wantAffectedRows: 4,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.mockDB()
			res := tc.exec.Exec(context.Background())
			require.Equal(t, tc.wantErr, res.Err())
			if res.Err() != nil {
				return
			}

			affectRows, err := res.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.wantAffectedRows, affectRows)
		})
	}
}

func TestShardingUpdaterSuite(t *testing.T) {
	suite.Run(t, &ShardingUpdaterSuite{})
}

func ExampleShardingUpdater_SkipNilValue() {
	r := model.NewMetaRegistry()
	_, _ = r.Register(&OrderDetail{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "OrderId",
			DBPattern:    &hash.Pattern{Name: "order_detail_db_%d", Base: 2},
			TablePattern: &hash.Pattern{Name: "order_detail_tab_%d", Base: 3},
			DsPattern:    &hash.Pattern{Name: "0.db.cluster.company.com:3306", NotSharding: true},
		}))
	m := map[string]*masterslave.MasterSlavesDB{
		"order_detail_db_1": MasterSlavesMemoryDB(),
	}
	clusterDB := cluster.NewClusterDB(m)
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
	}
	shardingDB, _ := OpenDS("sqlite3",
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r))
	query, _ := NewShardingUpdater[OrderDetail](shardingDB).Update(&OrderDetail{
		UsingCol1: "Jack", ItemId: 11,
	}).SkipNilValue().Where(C("OrderId").EQ(1)).Build(context.Background())
	fmt.Println(query[0].String())

	// Output:
	// SQL: UPDATE `order_detail_db_1`.`order_detail_tab_1` SET `item_id`=?,`using_col1`=? WHERE `order_id`=?;
	// Args: []interface {}{11, "Jack", 1}
}

func ExampleShardingUpdater_SkipZeroValue() {
	r := model.NewMetaRegistry()
	_, _ = r.Register(&OrderDetail{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "OrderId",
			DBPattern:    &hash.Pattern{Name: "order_detail_db_%d", Base: 2},
			TablePattern: &hash.Pattern{Name: "order_detail_tab_%d", Base: 3},
			DsPattern:    &hash.Pattern{Name: "0.db.cluster.company.com:3306", NotSharding: true},
		}))
	m := map[string]*masterslave.MasterSlavesDB{
		"order_detail_db_1": MasterSlavesMemoryDB(),
	}
	clusterDB := cluster.NewClusterDB(m)
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
	}
	shardingDB, _ := OpenDS("sqlite3",
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r))
	query, _ := NewShardingUpdater[OrderDetail](shardingDB).Update(&OrderDetail{
		UsingCol1: "Jack",
	}).SkipZeroValue().Where(C("OrderId").EQ(1)).Build(context.Background())
	fmt.Println(query[0].String())

	// Output:
	// SQL: UPDATE `order_detail_db_1`.`order_detail_tab_1` SET `using_col1`=? WHERE `order_id`=?;
	// Args: []interface {}{"Jack", 1}
}

type OrderDetail struct {
	OrderId   int `eorm:"auto_increment,primary_key"`
	ItemId    int
	UsingCol1 string
	UsingCol2 *sql.NullString
}
