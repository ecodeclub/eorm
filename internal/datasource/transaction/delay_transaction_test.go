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
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/cluster"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves/roundrobin"
	"github.com/ecodeclub/eorm/internal/datasource/shardingsource"
	"github.com/ecodeclub/eorm/internal/datasource/transaction"
	"github.com/ecodeclub/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
)

type TestDelayTxTestSuite struct {
	suite.Suite
	datasource.DataSource
	mockMaster1DB *sql.DB
	mockMaster    sqlmock.Sqlmock

	mockSlave1DB *sql.DB
	mockSlave1   sqlmock.Sqlmock

	mockSlave2DB *sql.DB
	mockSlave2   sqlmock.Sqlmock

	mockSlave3DB *sql.DB
	mockSlave3   sqlmock.Sqlmock

	mockMaster2DB *sql.DB
	mockMaster2   sqlmock.Sqlmock

	mockSlave4DB *sql.DB
	mockSlave4   sqlmock.Sqlmock

	mockSlave5DB *sql.DB
	mockSlave5   sqlmock.Sqlmock

	mockSlave6DB *sql.DB
	mockSlave6   sqlmock.Sqlmock
}

func (s *TestDelayTxTestSuite) SetupTest() {
	t := s.T()
	s.initMock(t)
}

func (s *TestDelayTxTestSuite) TearDownTest() {
	_ = s.mockMaster1DB.Close()
	_ = s.mockSlave1DB.Close()
	_ = s.mockSlave2DB.Close()
	_ = s.mockSlave3DB.Close()

	_ = s.mockMaster2DB.Close()
	_ = s.mockSlave4DB.Close()
	_ = s.mockSlave5DB.Close()
	_ = s.mockSlave6DB.Close()
}

func (s *TestDelayTxTestSuite) initMock(t *testing.T) {
	var err error
	s.mockMaster1DB, s.mockMaster, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockSlave1DB, s.mockSlave1, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockSlave2DB, s.mockSlave2, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockSlave3DB, s.mockSlave3, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	s.mockMaster2DB, s.mockMaster2, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockSlave4DB, s.mockSlave4, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockSlave5DB, s.mockSlave5, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockSlave6DB, s.mockSlave6, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	db1 := masterslave.NewMasterSlavesDB(s.mockMaster1DB, masterslave.MasterSlavesWithSlaves(
		s.newSlaves(s.mockSlave1DB, s.mockSlave2DB, s.mockSlave3DB)))

	db2 := masterslave.NewMasterSlavesDB(s.mockMaster2DB, masterslave.MasterSlavesWithSlaves(
		s.newSlaves(s.mockSlave4DB, s.mockSlave5DB, s.mockSlave6DB)))

	clusterDB1 := cluster.NewClusterDB(map[string]*masterslave.MasterSlavesDB{"db_0": db1})
	clusterDB2 := cluster.NewClusterDB(map[string]*masterslave.MasterSlavesDB{"db_0": db2})

	s.DataSource = shardingsource.NewShardingDataSource(map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB1,
		"1.db.cluster.company.com:3306": clusterDB2,
	})
}

func (s *TestDelayTxTestSuite) TestShardingInsert_Commit_Or_Rollback() {
	t := s.T()
	beginner := s.DataSource.(datasource.TxBeginner)
	testCases := []struct {
		name         string
		wantAffected int64
		values       []*test.OrderDetail
		querySet     []*test.OrderDetail
		tx           datasource.Tx
		afterFunc    func(t *testing.T, tx datasource.Tx, values []*test.OrderDetail)
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
			tx: func() datasource.Tx {

				tx, er := beginner.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
				require.NoError(t, er)
				return tx
			}(),
			afterFunc: func(t *testing.T, tx datasource.Tx, values []*test.OrderDetail) {
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
			tx: func() datasource.Tx {
				tx, er := beginner.BeginTx(
					transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
				require.NoError(t, er)
				return tx
			}(),
			afterFunc: func(t *testing.T, tx datasource.Tx, values []*test.OrderDetail) {
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
			tx := tc.tx
			querySet, err := eorm.NewShardingSelector[test.OrderDetail](tx).
				Where(eorm.C("OrderId").NEQ(123)).
				GetMulti(masterslave.UseMaster(context.Background()))
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.querySet, querySet)
			//res := eorm.NewShardingInsert[test.OrderDetail](tx).
			//	Values(ts.values).Exec(context.Background())
			//affected, err := res.RowsAffected()
			//require.NoError(t, err)
			//assert.Equal(t, ts.wantAffected, affected)
			//ts.afterFunc(t, tx, ts.values)
		})
	}
}

func (s *TestDelayTxTestSuite) newSlaves(dbs ...*sql.DB) slaves.Slaves {
	res, err := roundrobin.NewSlaves(dbs...)
	require.NoError(s.T(), err)
	return res
}

func TestTransactionSuite(t *testing.T) {
	suite.Run(t, &TestDelayTxTestSuite{})
}
