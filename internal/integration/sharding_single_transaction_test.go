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
//
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

type ShardingSingleTxTestSuite struct {
	ShardingSelectUpdateInsertSuite
}

func (s *ShardingSingleTxTestSuite) TestDoubleShardingSelect() {
	t := s.T()
	testCases := []struct {
		name     string
		querySet []*test.OrderDetail
		txFunc   func(t *testing.T) *eorm.Tx
	}{
		{
			name: "double select",
			querySet: []*test.OrderDetail{
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
			},
			txFunc: func(t *testing.T) *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Single), &sql.TxOptions{})
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
				Where(eorm.C("OrderId").EQ(123)).
				GetMulti(masterslave.UseMaster(context.Background()))
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.querySet, querySet)

			querySet, err = eorm.NewShardingSelector[test.OrderDetail](tx).
				Where(eorm.C("OrderId").EQ(123)).
				GetMulti(masterslave.UseMaster(context.Background()))
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.querySet, querySet)
		})
	}
}

func (s *ShardingSingleTxTestSuite) TestShardingSelectInsert_Commit_Or_Rollback() {
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
			wantAffected: 1,
			values: []*test.OrderDetail{
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
			},
			txFunc: func(t *testing.T) *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Single), &sql.TxOptions{})
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
			wantAffected: 1,
			values: []*test.OrderDetail{
				{OrderId: 199, ItemId: 100, UsingCol1: "Jason", UsingCol2: "Tatum"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
			},
			txFunc: func(t *testing.T) *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Single), &sql.TxOptions{})
				require.NoError(t, er)
				return tx
			},
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				var wantOds []*test.OrderDetail
				err := tx.Rollback()
				require.NoError(t, err)

				queryVal := s.findTgt(t, values)
				assert.ElementsMatch(t, wantOds, queryVal)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tx := tc.txFunc(t)
			querySet, err := eorm.NewShardingSelector[test.OrderDetail](tx).
				Where(eorm.C("OrderId").EQ(123)).
				GetMulti(masterslave.UseMaster(context.Background()))
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.querySet, querySet)
			res := eorm.NewShardingInsert[test.OrderDetail](tx).
				Values(tc.values).Exec(context.Background())
			affected, err := res.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.wantAffected, affected)
			tc.afterFunc(t, tx, tc.values)
		})
	}
}

func (s *ShardingSingleTxTestSuite) TestShardingSelectUpdate_Commit_Or_Rollback() {
	t := s.T()
	testCases := []struct {
		name         string
		wantAffected int64
		target       *test.OrderDetail
		upPre        eorm.Predicate
		querySet     []*test.OrderDetail
		upQuerySet   []*test.OrderDetail
		txFunc       func(t *testing.T) *eorm.Tx
		afterFunc    func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail)
	}{
		{
			name:         "select update where eq commit",
			upPre:        eorm.C("OrderId").EQ(11),
			wantAffected: 1,
			target:       &test.OrderDetail{UsingCol1: "ben"},
			querySet: []*test.OrderDetail{
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
			},
			upQuerySet: []*test.OrderDetail{
				{OrderId: 11, ItemId: 8, UsingCol1: "ben", UsingCol2: "Harden"},
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
			},
			txFunc: func(t *testing.T) *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Single), &sql.TxOptions{})
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
			name:         "select update rollback",
			upPre:        eorm.C("OrderId").EQ(181),
			wantAffected: 1,
			target:       &test.OrderDetail{UsingCol1: "Jordan"},
			querySet: []*test.OrderDetail{
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
			},
			upQuerySet: []*test.OrderDetail{
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
			},
			txFunc: func(t *testing.T) *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Single), &sql.TxOptions{})
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
				Where(eorm.C("OrderId").EQ(123)).
				GetMulti(masterslave.UseMaster(context.Background()))
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.querySet, querySet)
			res := eorm.NewShardingUpdater[test.OrderDetail](tx).Update(tc.target).
				Set(eorm.C("UsingCol1")).Where(tc.upPre).Exec(context.Background())
			affected, err := res.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.wantAffected, affected)
			tc.afterFunc(t, tx, tc.upQuerySet)
		})
	}
}

func TestMySQL8ShardingSingleTxTestSuite(t *testing.T) {
	suite.Run(t, &ShardingSingleTxTestSuite{
		ShardingSelectUpdateInsertSuite: newShardingSelectUpdateInsertSuite(),
	})
}
