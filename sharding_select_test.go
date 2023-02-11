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

package eorm

import (
	"fmt"
	"testing"

	"github.com/gotomicro/eorm/internal/errs"
	"github.com/gotomicro/eorm/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardingSelector_Build(t *testing.T) {
	db := memoryDB()
	r := model.NewMetaRegistry()
	m, err := r.Register(&Order{},
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
	testCases := []struct {
		name    string
		builder ShardingQueryBuilder
		qs      []*ShardingQuery
		wantErr error
	}{
		{
			name: "only eq",
			builder: func() ShardingQueryBuilder {
				shardingDB := &ShardingDB{
					DBs: map[string]*DB{
						"order_db_1": db,
					},
				}
				s := NewShardingSelector[Order](shardingDB).RegisterTableMeta(m).Where(C("UserId").EQ(int64(123)))
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
				shardingDB := &ShardingDB{
					DBs: map[string]*DB{
						"order_db_1": db,
					},
				}
				s := NewShardingSelector[Order](shardingDB).RegisterTableMeta(m).
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
				shardingDB := &ShardingDB{
					DBs: map[string]*DB{
						"order_db_1": db,
					},
				}
				s := NewShardingSelector[Order](shardingDB).RegisterTableMeta(m).
					Select(C("Invalid")).Where(C("UserId").EQ(int64(123)))
				return s
			}(),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name: "order by",
			builder: func() ShardingQueryBuilder {
				shardingDB := &ShardingDB{
					DBs: map[string]*DB{
						"order_db_1": db,
					},
				}
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					RegisterTableMeta(m).Where(C("UserId").EQ(int64(123))).OrderBy(ASC("UserId"), DESC("OrderId"))
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
				shardingDB := &ShardingDB{
					DBs: map[string]*DB{
						"order_db_1": db,
					},
				}
				s := NewShardingSelector[Order](shardingDB).Select(C("OrderId"), C("Content")).
					RegisterTableMeta(m).Where(C("UserId").EQ(int64(123))).GroupBy("UserId", "OrderId")
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
				shardingDB := &ShardingDB{
					DBs: map[string]*DB{
						"order_db_1": db,
					},
				}
				s := NewShardingSelector[Order](shardingDB).RegisterTableMeta(m).
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
			name: "and left broadcast",
			builder: func() ShardingQueryBuilder {
				shardingDB := &ShardingDB{
					DBs: map[string]*DB{
						"order_db_1": db,
					},
				}
				s := NewShardingSelector[Order](shardingDB).RegisterTableMeta(m).
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
			name: "and right broadcast",
			builder: func() ShardingQueryBuilder {
				shardingDB := &ShardingDB{
					DBs: map[string]*DB{
						"order_db_1": db,
					},
				}
				s := NewShardingSelector[Order](shardingDB).RegisterTableMeta(m).
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
			name: "and empty",
			builder: func() ShardingQueryBuilder {
				shardingDB := &ShardingDB{
					DBs: map[string]*DB{
						"order_db_1": db,
					},
				}
				s := NewShardingSelector[Order](shardingDB).RegisterTableMeta(m).
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
			assert.EqualValues(t, c.qs, qs)
		})
	}
}

func TestShardingSelector_findDstByPredicate(t *testing.T) {
	db := memoryDB()
	r := model.NewMetaRegistry()
	m, err := r.Register(&Order{},
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
	shardingDB := &ShardingDB{
		DBs: map[string]*DB{
			"order_db_1": db,
		},
	}
	s := NewShardingSelector[Order](shardingDB).RegisterTableMeta(m)
	testCases := []struct {
		name     string
		p        Predicate
		wantDsts []Dst
		wantErr  error
	}{
		{
			name: "only eq",
			p:    C("UserId").EQ(int64(123)),
			wantDsts: []Dst{
				{
					DB:    "order_db_1",
					Table: "order_tab_3",
				},
			},
		},
		{
			name: "and left broadcast",
			p:    C("Id").EQ(12).And(C("UserId").EQ(int64(123))),
			wantDsts: []Dst{
				{
					DB:    "order_db_1",
					Table: "order_tab_3",
				},
			},
		},
		{
			name: "and right broadcast",
			p:    C("UserId").EQ(int64(123)).And(C("Id").EQ(12)),
			wantDsts: []Dst{
				{
					DB:    "order_db_1",
					Table: "order_tab_3",
				},
			},
		},
		{
			name:     "and empty",
			p:        C("UserId").EQ(int64(123)).And(C("UserId").EQ(int64(124))),
			wantDsts: []Dst{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dsts, err := s.findDstByPredicate(tc.p)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantDsts, dsts)
		})
	}
}

type Order struct {
	UserId  int64
	OrderId int64
	Content string
	Account float64
}
