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

package transaction_test

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/datasource/transaction"
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TestSingleTxTestSuite struct {
	ShardingTransactionSuite
}

func (s *TestSingleTxTestSuite) TestExecute_Commit_Or_Rollback() {
	t := s.T()
	testCases := []struct {
		name         string
		wantAffected int64
		wantErr      error
		shardingVal  int
		values       []*test.OrderDetail
		querySet     []*test.OrderDetail
		tx           *eorm.Tx
		mockOrder    func(mock1, mock2 sqlmock.Sqlmock)
		afterFunc    func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail)
	}{
		{
			name:         "select insert commit",
			wantAffected: 1,
			shardingVal:  234,
			values: []*test.OrderDetail{
				{OrderId: 288, ItemId: 101, UsingCol1: "Jimmy", UsingCol2: "Butler"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			},
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				mock1.MatchExpectationsInOrder(false)
				mock1.ExpectBegin()

				mock1.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`=?;")).
					WithArgs(234).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(234, 12, "Kevin", "Durant"))

				mock1.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_0`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(288, 101, "Jimmy", "Butler").WillReturnResult(sqlmock.NewResult(1, 1))

				mock1.ExpectCommit()
			},
			tx: func() *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Single), &sql.TxOptions{})
				require.NoError(t, er)
				return tx
			}(),
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Commit()
				require.NoError(t, err)

				s.mockMaster.MatchExpectationsInOrder(false)

				s.mockMaster.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`=?;")).
					WithArgs(288).WillReturnRows(s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(288, 101, "Jimmy", "Butler"))

				queryVal := s.findTgt(t, values)
				assert.ElementsMatch(t, values, queryVal)
			},
		},
		{
			name:         "select insert rollback",
			wantAffected: 1,
			shardingVal:  253,
			values: []*test.OrderDetail{
				{OrderId: 199, ItemId: 100, UsingCol1: "Jason", UsingCol2: "Tatum"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
			},
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				mock2.MatchExpectationsInOrder(false)
				mock2.ExpectBegin()

				mock2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_1` WHERE `order_id`=?;")).
					WithArgs(253).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(253, 8, "Stephen", "Curry"))

				mock2.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_1`.`order_detail_tab_1`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(199, 100, "Jason", "Tatum").WillReturnResult(sqlmock.NewResult(1, 1))

				mock2.ExpectRollback()
			},
			tx: func() *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Single), &sql.TxOptions{})
				require.NoError(t, er)
				return tx
			}(),
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Rollback()
				require.NoError(t, err)

				s.mockMaster2.MatchExpectationsInOrder(false)
				s.mockMaster2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_1` WHERE `order_id`=?;")).
					WithArgs(199).WillReturnRows(s.mockMaster2.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}))

				queryVal := s.findTgt(t, values)
				var wantOds []*test.OrderDetail
				assert.ElementsMatch(t, wantOds, queryVal)
			},
		},
		{
			name:         "insert use multi db err",
			wantAffected: 2,
			shardingVal:  234,
			wantErr:      errs.NewErrDBNotEqual("order_detail_db_0", "order_detail_db_1"),
			values: []*test.OrderDetail{
				{OrderId: 288, ItemId: 101, UsingCol1: "Jimmy", UsingCol2: "Butler"},
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			},
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				mock1.MatchExpectationsInOrder(false)
				mock2.MatchExpectationsInOrder(false)
				mock1.ExpectBegin()
				mock2.ExpectBegin()

				mock1.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`=?;")).
					WithArgs(234).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(234, 12, "Kevin", "Durant"))

				mock1.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_0`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(288, 101, "Jimmy", "Butler").WillReturnResult(sqlmock.NewResult(1, 1))
				mock2.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_1`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(33, 100, "Nikolai", "Jokic").WillReturnResult(sqlmock.NewResult(1, 1))

				commitErr := errors.New("commit fail")
				mock1.ExpectCommit().WillReturnError(commitErr)
				mock2.ExpectCommit().WillReturnError(commitErr)
			},
			tx: func() *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Single), &sql.TxOptions{})
				require.NoError(t, er)
				return tx
			}(),
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Commit()
				newErr := errors.New("commit fail")
				errSlice := strings.Split(err.Error(), "; ")
				wantErrSlice := []string{
					newMockCommitErr("order_detail_db_0", newErr).Error(),
					newMockCommitErr("order_detail_db_1", newErr).Error()}
				assert.ElementsMatch(t, wantErrSlice, errSlice)

				s.mockMaster.MatchExpectationsInOrder(false)
				s.mockMaster2.MatchExpectationsInOrder(false)

				//row1 := s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				//row2 := s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				//s.mockMaster.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_1` WHERE (`order_id`=?) OR (`order_id`=?);SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_2` WHERE (`order_id`=?) OR (`order_id`=?);")).
				//	WithArgs(288, 33, 288, 33, 288, 33).WillReturnRows(row1)
				//
				//s.mockMaster2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_1` WHERE (`order_id`=?) OR (`order_id`=?);SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_2` WHERE (`order_id`=?) OR (`order_id`=?);")).
				//	WithArgs(288, 33, 288, 33, 288, 33).WillReturnRows(row2)

				row1 := s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				row2 := s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				s.mockMaster.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);")).
					WithArgs(288, 33).WillReturnRows(row1)

				s.mockMaster2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);")).
					WithArgs(288, 33).WillReturnRows(row2)

				queryVal := s.findTgt(t, values)
				var wantOds []*test.OrderDetail
				assert.ElementsMatch(t, wantOds, queryVal)
			},
		},
		{
			name:         "select and insert use multi db err",
			wantAffected: 2,
			shardingVal:  234,
			wantErr:      errs.NewErrDBNotEqual("order_detail_db_0", "order_detail_db_1"),
			values: []*test.OrderDetail{
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			},
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				mock1.MatchExpectationsInOrder(false)
				mock1.ExpectBegin()

				mock1.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`=?;")).
					WithArgs(234).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(234, 12, "Kevin", "Durant"))

				mock1.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_1`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(33, 100, "Nikolai", "Jokic").WillReturnResult(sqlmock.NewResult(1, 1))

				mock1.ExpectCommit()
			},
			tx: func() *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Single), &sql.TxOptions{})
				require.NoError(t, er)
				return tx
			}(),
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Commit()
				newErr := errors.New("commit fail")
				errSlice := strings.Split(err.Error(), "; ")
				wantErrSlice := []string{
					newMockCommitErr("order_detail_db_0", newErr).Error(),
					newMockCommitErr("order_detail_db_1", newErr).Error()}
				assert.ElementsMatch(t, wantErrSlice, errSlice)

				s.mockMaster.MatchExpectationsInOrder(false)
				s.mockMaster2.MatchExpectationsInOrder(false)

				row1 := s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				row2 := s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				//s.mockMaster.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_1` WHERE (`order_id`=?) OR (`order_id`=?);SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_2` WHERE (`order_id`=?) OR (`order_id`=?);")).
				//	WithArgs(288, 33, 288, 33, 288, 33).WillReturnRows(row1)
				//
				//s.mockMaster2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_1` WHERE (`order_id`=?) OR (`order_id`=?);SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_2` WHERE (`order_id`=?) OR (`order_id`=?);")).
				//	WithArgs(288, 33, 288, 33, 288, 33).WillReturnRows(row2)

				s.mockMaster.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`=?;")).
					WithArgs(33).WillReturnRows(row1)

				s.mockMaster2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE `order_id`=?;")).
					WithArgs(33).WillReturnRows(row2)

				queryVal := s.findTgt(t, values)
				var wantOds []*test.OrderDetail
				assert.ElementsMatch(t, wantOds, queryVal)
			},
		},
		{
			name:         "select insert commit err",
			wantAffected: 1,
			shardingVal:  234,
			values: []*test.OrderDetail{
				{OrderId: 288, ItemId: 101, UsingCol1: "Jimmy", UsingCol2: "Butler"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			},
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				mock1.MatchExpectationsInOrder(false)
				mock1.ExpectBegin()

				mock1.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`=?;")).
					WithArgs(234).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(234, 12, "Kevin", "Durant"))

				mock1.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_0`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(288, 101, "Jimmy", "Butler").WillReturnResult(sqlmock.NewResult(1, 1))

				mock1.ExpectCommit().WillReturnError(errors.New("commit fail"))
			},
			tx: func() *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Single), &sql.TxOptions{})
				require.NoError(t, er)
				return tx
			}(),
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Commit()
				wantErr := errors.New("commit fail")
				assert.Equal(t, wantErr, err)

				s.mockMaster.MatchExpectationsInOrder(false)
				s.mockMaster.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`=?;")).
					WithArgs(288).WillReturnRows(s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}))

				queryVal := s.findTgt(t, values)
				var wantOds []*test.OrderDetail
				assert.ElementsMatch(t, wantOds, queryVal)
			},
		},
		{
			name:         "select insert rollback err",
			wantAffected: 1,
			shardingVal:  253,
			values: []*test.OrderDetail{
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
			},
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				mock2.MatchExpectationsInOrder(false)
				mock2.ExpectBegin()

				mock2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_1` WHERE `order_id`=?;")).
					WithArgs(253).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(253, 8, "Stephen", "Curry"))

				mock2.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_1`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(33, 100, "Nikolai", "Jokic").WillReturnResult(sqlmock.NewResult(1, 1))

				mock2.ExpectRollback().WillReturnError(errors.New("rollback fail"))
			},
			tx: func() *eorm.Tx {
				tx, er := s.shardingDB.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Single), &sql.TxOptions{})
				require.NoError(t, er)
				return tx
			}(),
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Rollback()
				wantErr := errors.New("rollback fail")
				assert.Equal(t, wantErr, err)

				s.mockMaster2.MatchExpectationsInOrder(false)

				s.mockMaster2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE `order_id`=?")).
					WithArgs(33).WillReturnRows(s.mockMaster2.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}))

				queryVal := s.findTgt(t, values)
				var wantOds []*test.OrderDetail
				assert.ElementsMatch(t, wantOds, queryVal)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.mockOrder(s.mockMaster, s.mockMaster2)
			tx := tc.tx
			querySet, err := eorm.NewShardingSelector[test.OrderDetail](tx).
				Where(eorm.C("OrderId").EQ(tc.shardingVal)).
				GetMulti(masterslave.UseMaster(context.Background()))
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.querySet, querySet)

			values := tc.values
			res := eorm.NewShardingInsert[test.OrderDetail](tx).
				Values(values).Exec(context.Background())
			affected, err := res.RowsAffected()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantAffected, affected)
			tc.afterFunc(t, tx, values)
		})
	}
}

func TestSingleTransactionSuite(t *testing.T) {
	suite.Run(t, &TestSingleTxTestSuite{
		ShardingTransactionSuite: newShardingTransactionSuite(),
	})
}
