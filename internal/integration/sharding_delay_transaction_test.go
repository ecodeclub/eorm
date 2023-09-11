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
	"testing"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/datasource/transaction"

	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ShardingDelayTxTestSuite struct {
	ShardingSelectUpdateInsertSuite
}

func (s *ShardingDelayTxTestSuite) TestDoubleShardingSelect() {
	t := s.T()
	testCases := []struct {
		name     string
		wantErr  error
		querySet []*test.OrderDetail
		txFunc   func(t *testing.T) *eorm.Tx
	}{
		{
			name: "double select",
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
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tx := tc.txFunc(t)
			defer tx.Commit()
			querySet, err := eorm.NewShardingSelector[test.OrderDetail](tx).
				Where(eorm.C("OrderId").NEQ(123)).
				GetMulti(masterslave.UseMaster(context.Background()))
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.querySet, querySet)

			querySet, err = eorm.NewShardingSelector[test.OrderDetail](tx).
				Where(eorm.C("OrderId").NEQ(123)).
				GetMulti(masterslave.UseMaster(context.Background()))
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.querySet, querySet)
		})
	}
}

func (s *ShardingDelayTxTestSuite) TestShardingSelectUpdateInsert_Commit_Or_Rollback() {
	t := s.T()
	testCases := []struct {
		name           string
		updateAffected int64
		insertAffected int64
		target         *test.OrderDetail
		upPre          eorm.Predicate
		insertValues   []*test.OrderDetail
		querySet       []*test.OrderDetail
		upQuerySet     []*test.OrderDetail
		txFunc         func(t *testing.T) *eorm.Tx
		afterFunc      func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail)
	}{
		{
			name:           "select insert update commit",
			upPre:          eorm.C("OrderId").EQ(181),
			updateAffected: 1,
			insertAffected: 2,
			target:         &test.OrderDetail{UsingCol1: "Jordan"},
			insertValues: []*test.OrderDetail{
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
				{OrderId: 288, ItemId: 101, UsingCol1: "Jimmy", UsingCol2: "Butler"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
			},
			upQuerySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Jordan", UsingCol2: "Leonard"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 288, ItemId: 101, UsingCol1: "Jimmy", UsingCol2: "Butler"},
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
			name:           "select insert update broadcast commit",
			upPre:          eorm.C("OrderId").GTEQ(253),
			updateAffected: 2,
			insertAffected: 2,
			target:         &test.OrderDetail{UsingCol1: "Jordan"},
			insertValues: []*test.OrderDetail{
				{OrderId: 199, ItemId: 100, UsingCol1: "Jason", UsingCol2: "Tatum"},
				{OrderId: 299, ItemId: 101, UsingCol1: "Paul", UsingCol2: "George"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Jordan", UsingCol2: "Leonard"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 288, ItemId: 101, UsingCol1: "Jimmy", UsingCol2: "Butler"},
			},
			upQuerySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Jordan", UsingCol2: "Leonard"},
				{OrderId: 199, ItemId: 100, UsingCol1: "Jason", UsingCol2: "Tatum"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Jordan", UsingCol2: "Curry"},
				{OrderId: 288, ItemId: 101, UsingCol1: "Jordan", UsingCol2: "Butler"},
				{OrderId: 299, ItemId: 101, UsingCol1: "Paul", UsingCol2: "George"},
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
			name:           "select insert update rollback",
			upPre:          eorm.C("OrderId").EQ(299),
			updateAffected: 1,
			insertAffected: 1,
			target:         &test.OrderDetail{UsingCol1: "Jordan"},
			insertValues: []*test.OrderDetail{
				{OrderId: 48, ItemId: 100},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Jordan", UsingCol2: "Leonard"},
				{OrderId: 199, ItemId: 100, UsingCol1: "Jason", UsingCol2: "Tatum"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Jordan", UsingCol2: "Curry"},
				{OrderId: 288, ItemId: 101, UsingCol1: "Jordan", UsingCol2: "Butler"},
				{OrderId: 299, ItemId: 101, UsingCol1: "Paul", UsingCol2: "George"},
			},
			upQuerySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Jordan", UsingCol2: "Leonard"},
				{OrderId: 199, ItemId: 100, UsingCol1: "Jason", UsingCol2: "Tatum"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Jordan", UsingCol2: "Curry"},
				{OrderId: 288, ItemId: 101, UsingCol1: "Jordan", UsingCol2: "Butler"},
				{OrderId: 299, ItemId: 101, UsingCol1: "Paul", UsingCol2: "George"},
			},
			txFunc: func(t *testing.T) *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
				require.NoError(t, er)
				return tx
			},
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Rollback()
				require.NoError(t, err)

				queryVal := s.findTgt(t, values)
				assert.ElementsMatch(t, values, queryVal)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tx := tc.txFunc(t)
			querySet, err := eorm.NewShardingSelector[test.OrderDetail](tx).
				Where(eorm.C("OrderId").NEQ(123)).
				GetMulti(masterslave.UseMaster(context.Background()))
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.querySet, querySet)

			res := eorm.NewShardingUpdater[test.OrderDetail](tx).Update(tc.target).
				Set(eorm.C("UsingCol1")).Where(tc.upPre).Exec(context.Background())
			affected, err := res.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.updateAffected, affected)

			res = eorm.NewShardingInsert[test.OrderDetail](tx).
				Values(tc.insertValues).Exec(context.Background())
			affected, err = res.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.insertAffected, affected)

			tc.afterFunc(t, tx, tc.upQuerySet)
		})
	}
}

func TestMySQL8ShardingDelayTxTestSuite(t *testing.T) {
	suite.Run(t, &ShardingDelayTxTestSuite{
		ShardingSelectUpdateInsertSuite: newShardingSelectUpdateInsertSuite(),
	})
}
