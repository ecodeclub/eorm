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
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/datasource/transaction"
	operator "github.com/ecodeclub/eorm/internal/operator"

	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/sharding"
	"github.com/ecodeclub/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ShardingDelayTxTestSuite struct {
	ShardingSuite
	data []*test.OrderDetail
}

func (s *ShardingDelayTxTestSuite) SetupSuite() {
	t := s.T()
	s.ShardingSuite.SetupSuite()
	for _, item := range s.data {
		shardingRes, err := s.algorithm.Sharding(
			context.Background(), sharding.Request{Op: operator.OpEQ, SkValues: map[string]any{s.ShardingKey: item.OrderId}})
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

func (s *ShardingDelayTxTestSuite) TestShardingInsert_Commit_Or_Rollback() {
	t := s.T()
	testCases := []struct {
		name         string
		wantAffected int64
		values       []*test.OrderDetail
		querySet     []*test.OrderDetail
		txFunc       func(t *testing.T) *eorm.Tx
		afterFunc    func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail)
	}{
		{
			name:         "select insert commit",
			wantAffected: 2,
			values: []*test.OrderDetail{
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
				{OrderId: 288, ItemId: 101, UsingCol1: "Jimmy", UsingCol2: "Butler"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
			},
			txFunc: func(t *testing.T) *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
				require.NoError(t, er)
				return tx
			},
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Commit()
				require.NoError(t, err)

				queryVal := s.findTgt(t, values)
				assert.ElementsMatch(t, values, queryVal)
			},
		},
		{
			name:         "select insert rollback",
			wantAffected: 2,
			values: []*test.OrderDetail{
				{OrderId: 199, ItemId: 100, UsingCol1: "Jason", UsingCol2: "Tatum"},
				{OrderId: 299, ItemId: 101, UsingCol1: "Paul", UsingCol2: "George"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
			},
			txFunc: func(t *testing.T) *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
				require.NoError(t, er)
				return tx
			},
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				var wantOds []*test.OrderDetail
				err := tx.Rollback()
				require.NoError(t, err)

				queryVal := s.findTgt(t, values)
				for i := 0; i < len(values); i++ {
					wantOds = append(wantOds, nil)
				}

				assert.ElementsMatch(t, wantOds, queryVal)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tx := tc.txFunc(t)
			defer tx.Commit()
			//err := txFunc.Rollback()
			//require.NoError(t, err)
			//fmt.Println(1111)
			querySet, err := eorm.NewShardingSelector[test.OrderDetail](tx).
				Where(eorm.C("OrderId").NEQ(123)).
				GetMulti(masterslave.UseMaster(context.Background()))
			//fmt.Println(2222)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.querySet, querySet)
			//res := eorm.NewShardingInsert[test.OrderDetail](txFunc).
			//	Values(tc.values).Exec(context.Background())
			//affected, err := res.RowsAffected()
			//require.NoError(t, err)
			//assert.Equal(t, tc.wantAffected, affected)
			//tc.afterFunc(t, txFunc, tc.values)
		})
	}
}

func (s *ShardingDelayTxTestSuite) findTgt(t *testing.T, values []*test.OrderDetail) []*test.OrderDetail {
	od := values[0]
	pre := eorm.C(s.ShardingKey).EQ(od.OrderId)
	for i := 1; i < len(values); i++ {
		od = values[i]
		pre = pre.Or(eorm.C(s.ShardingKey).EQ(od.OrderId))
	}
	querySet, err := eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
		Where(pre).GetMulti(masterslave.UseMaster(context.Background()))
	require.NoError(t, err)
	return querySet
}

//func (s *ShardingDelayTxTestSuite) TestShardingUpdate_Commit() {
//	t := s.T()
//	r := model.NewMetaRegistry()
//	_, err := r.Register(&test.OrderDetail{},
//		model.WithTableShardingAlgorithm(s.algorithm))
//	require.NoError(t, err)
//	eorm.DBOptionWithMetaRegistry(r)(s.shardingDB)
//	testCases := []struct {
//		name    string
//		wantErr error
//		//exec             sharding.Executor
//		//updatedSelector  *eorm.ShardingSelector[test.OrderDetail]
//		execBuilder     sharding.QueryBuilder
//		selectorBuilder *eorm.ShardingSelector[test.OrderDetail]
//		updatedQuerySet []*test.OrderDetail
//		querySet        []*test.OrderDetail
//		txFunc              *eorm.Tx
//	}{
//		{
//			name: "select update",
//			execBuilder: eorm.NewShardingUpdater[test.OrderDetail](s.shardingDB).Update(&test.OrderDetail{
//				ItemId: 112, UsingCol1: "King", UsingCol2: "James",
//			}).Set(eorm.Columns("ItemId", "UsingCol1", "UsingCol2")).
//				Where(eorm.C("OrderId").EQ(123).Or(eorm.C("ItemId").EQ(11))),
//			selectorBuilder: eorm.NewShardingSelector[test.OrderDetail](s.shardingDB).
//				Where(eorm.C("OrderId").NotIn(123, 181)),
//			updatedQuerySet: []*test.OrderDetail{
//				{OrderId: 123, ItemId: 112, UsingCol1: "King", UsingCol2: "James"},
//				{OrderId: 181, ItemId: 112, UsingCol1: "King", UsingCol2: "James"},
//			},
//			querySet: []*test.OrderDetail{
//				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
//				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
//				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
//				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
//			},
//		},
//		txFunc: func() *eorm.Tx {
//			txFunc, er := s.shardingDB.BeginTx(transaction.UsingTxType(context.Background()), &sql.TxOptions{})
//			require.NoError(t, er)
//		}(),
//	}
//	for _, tc := range testCases {
//		t.Run(tc.name, func(t *testing.T) {
//			txFunc := tc.txFunc
//
//			rows, queryErr := txFunc.queryContext(context.Background(), datasource.Query(tc.query))
//
//			res := tc.exec.Exec(context.Background())
//			require.Equal(t, tc.wantErr, res.Err())
//			if res.Err() != nil {
//				return
//			}
//
//			affectRows, err := res.RowsAffected()
//			require.NoError(t, err)
//			assert.Equal(t, tc.wantAffectedRows, affectRows)
//
//			// TODO 从库测试目前有查不到数据的bug
//			ctx := masterslave.UseMaster(context.Background())
//			updatedQuerySet, err := tc.updatedSelector.GetMulti(ctx)
//			require.NoError(t, err)
//			assert.ElementsMatch(t, tc.updatedQuerySet, updatedQuerySet)
//
//			querySet, err := tc.selector.GetMulti(ctx)
//			require.NoError(t, err)
//			assert.ElementsMatch(t, tc.querySet, querySet)
//
//		})
//	}
//}

func (s *ShardingDelayTxTestSuite) TearDownSuite() {
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

func TestMySQL8DelayShardingTxTestSuite(t *testing.T) {
	suite.Run(t, &ShardingDelayTxTestSuite{
		ShardingSuite: newDefaultShardingSuite(),
		data: []*test.OrderDetail{
			{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
			{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
			{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
			{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
			{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
		},
	})
}
