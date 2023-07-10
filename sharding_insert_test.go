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
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/cluster"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/datasource/shardingsource"
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/model"
	"github.com/ecodeclub/eorm/internal/sharding"
	"github.com/ecodeclub/eorm/internal/sharding/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/multierr"
)

func newMockErr(dbName string) error {
	return fmt.Errorf("mock error for %s", dbName)
}

type OrderInsert struct {
	UserId  int `eorm:"primary_key"`
	OrderId int64
	Content string
	Account float64
}

func TestShardingInsert_Build(t *testing.T) {
	r := model.NewMetaRegistry()
	dbBase, tableBase, dsBase := 2, 3, 2
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab_%d", "%d.db.cluster.company.com:3306"
	_, err := r.Register(&OrderInsert{},
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
		wantQs  []sharding.Query
		wantErr error
	}{
		{
			name: "插入一个元素",
			builder: NewShardingInsert[OrderInsert](shardingDB).Values([]*OrderInsert{
				{UserId: 1, OrderId: 1, Content: "1", Account: 1.0},
			}),
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);", "`order_db_1`", "`order_tab_1`"),
					Args:       []any{1, int64(1), "1", 1.0},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "插入多个元素",
			builder: NewShardingInsert[OrderInsert](shardingDB).Values([]*OrderInsert{
				{UserId: 1, OrderId: 1, Content: "1", Account: 1.0},
				{UserId: 2, OrderId: 2, Content: "2", Account: 2.0},
				{UserId: 3, OrderId: 3, Content: "3", Account: 3.0},
				{UserId: 4, OrderId: 4, Content: "4", Account: 4.0},
			}),
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);", "`order_db_0`", "`order_tab_1`"),
					Args:       []any{4, int64(4), "4", 4.0},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);", "`order_db_0`", "`order_tab_2`"),
					Args:       []any{2, int64(2), "2", 2.0},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);", "`order_db_1`", "`order_tab_0`"),
					Args:       []any{3, int64(3), "3", 3.0},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);", "`order_db_1`", "`order_tab_1`"),
					Args:       []any{1, int64(1), "1", 1.0},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "插入多个元素, 但是不同的元素会被分配到同一个库",
			builder: NewShardingInsert[OrderInsert](shardingDB).Values([]*OrderInsert{
				{UserId: 1, OrderId: 1, Content: "1", Account: 1.0},
				{UserId: 7, OrderId: 7, Content: "7", Account: 7.0},
			}),
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?),(?,?,?,?);", "`order_db_1`", "`order_tab_1`"),
					Args:       []any{1, int64(1), "1", 1.0, 7, int64(7), "7", 7.0},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "插入多个元素, 有不同的元素会被分配到同一个库表",
			builder: NewShardingInsert[OrderInsert](shardingDB).Values([]*OrderInsert{
				{UserId: 1, OrderId: 1, Content: "1", Account: 1.0},
				{UserId: 7, OrderId: 7, Content: "7", Account: 7.0},
				{UserId: 2, OrderId: 2, Content: "2", Account: 2.0},
				{UserId: 8, OrderId: 8, Content: "8", Account: 8.0},
				{UserId: 3, OrderId: 3, Content: "3", Account: 3.0},
			}),
			wantQs: []sharding.Query{

				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?),(?,?,?,?);", "`order_db_0`", "`order_tab_2`"),
					Args:       []any{2, int64(2), "2", 2.0, 8, int64(8), "8", 8.0},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);", "`order_db_1`", "`order_tab_0`"),
					Args:       []any{3, int64(3), "3", 3.0},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?),(?,?,?,?);", "`order_db_1`", "`order_tab_1`"),
					Args:       []any{1, int64(1), "1", 1.0, 7, int64(7), "7", 7.0},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "插入时，插入的列没有包含分库分表的列",
			builder: NewShardingInsert[OrderInsert](shardingDB).Values([]*OrderInsert{
				{OrderId: 1, Content: "1", Account: 1.0},
			}).Columns([]string{"OrderId", "Content", "Account"}),
			wantErr: errs.ErrInsertShardingKeyNotFound,
		},
		{
			name: "插入时,忽略主键，但主键为shardingKey报错",
			builder: NewShardingInsert[OrderInsert](shardingDB).Values([]*OrderInsert{
				{OrderId: 1, Content: "1", Account: 1.0},
			}).IgnorePK(),
			wantErr: errs.ErrInsertShardingKeyNotFound,
		},
		{
			name:    "values中没有元素报错",
			builder: NewShardingInsert[OrderInsert](shardingDB),
			wantErr: errors.New("插入0行"),
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

type ShardingInsertSuite struct {
	suite.Suite
	mock01   sqlmock.Sqlmock
	mockDB01 *sql.DB
	mock02   sqlmock.Sqlmock
	mockDB02 *sql.DB
}

func (s *ShardingInsertSuite) SetupSuite() {
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

func (s *ShardingInsertSuite) TearDownTest() {
	_ = s.mockDB01.Close()
	_ = s.mockDB02.Close()
}

func (s *ShardingInsertSuite) TestShardingInsert_Exec() {
	r := model.NewMetaRegistry()
	dbBase, tableBase := 2, 3
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab_%d", "0.db.cluster.company.com:3306"
	_, err := r.Register(&OrderInsert{},
		model.WithTableShardingAlgorithm(&hash.Hash{
			ShardingKey:  "UserId",
			DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
			TablePattern: &hash.Pattern{Name: tablePattern, Base: tableBase},
			DsPattern:    &hash.Pattern{Name: dsPattern, NotSharding: true},
		}))
	require.NoError(s.T(), err)

	m := map[string]*masterslave.MasterSlavesDB{
		"order_db_0": MasterSlavesMockDB(s.mockDB01),
		"order_db_1": MasterSlavesMockDB(s.mockDB02),
	}
	clusterDB := cluster.NewClusterDB(m)
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
	}
	shardingDB, err := OpenDS("mysql",
		shardingsource.NewShardingDataSource(ds), DBWithMetaRegistry(r))
	require.NoError(s.T(), err)
	testcases := []struct {
		name             string
		si               *ShardingInserter[OrderInsert]
		mockDb           func()
		wantErr          error
		wantAffectedRows int64
	}{
		{
			name: "跨表插入全部成功",
			si: NewShardingInsert[OrderInsert](shardingDB).Values([]*OrderInsert{
				{UserId: 1, OrderId: 1, Content: "1", Account: 1.0},
				{UserId: 2, OrderId: 2, Content: "2", Account: 2.0},
				{UserId: 3, OrderId: 3, Content: "3", Account: 3.0},
			}),
			mockDb: func() {
				s.mock02.MatchExpectationsInOrder(false)
				s.mock02.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_1`.`order_tab_1`(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);")).WithArgs(1, int64(1), "1", 1.0).WillReturnResult(sqlmock.NewResult(1, 1))
				s.mock02.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_1`.`order_tab_0`(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);")).WithArgs(3, int64(3), "3", 3.0).WillReturnResult(sqlmock.NewResult(1, 1))
				s.mock01.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_0`.`order_tab_2`(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);")).WithArgs(2, int64(2), "2", 2.0).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			wantAffectedRows: 3,
		},
		{
			name: "部分插入失败",
			si: NewShardingInsert[OrderInsert](shardingDB).Values([]*OrderInsert{
				{UserId: 1, OrderId: 1, Content: "1", Account: 1.0},
				{UserId: 2, OrderId: 2, Content: "2", Account: 2.0},
				{UserId: 3, OrderId: 3, Content: "3", Account: 3.0},
			}),
			mockDb: func() {
				s.mock02.MatchExpectationsInOrder(false)
				s.mock02.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_1`.`order_tab_1`(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);")).WithArgs(1, int64(1), "1", 1.0).WillReturnError(newMockErr("db01"))
				s.mock02.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_1`.`order_tab_0`(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);")).WithArgs(3, int64(3), "3", 3.0).WillReturnResult(sqlmock.NewResult(1, 1))
				s.mock01.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_0`.`order_tab_2`(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);")).WithArgs(2, int64(2), "2", 2.0).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			wantErr: multierr.Combine(newMockErr("db01")),
		},
		{
			name: "全部插入失败",
			si: NewShardingInsert[OrderInsert](shardingDB).Values([]*OrderInsert{
				{UserId: 1, OrderId: 1, Content: "1", Account: 1.0},
				{UserId: 2, OrderId: 2, Content: "2", Account: 2.0},
				{UserId: 3, OrderId: 3, Content: "3", Account: 3.0},
			}),
			mockDb: func() {
				s.mock02.MatchExpectationsInOrder(false)
				s.mock02.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_1`.`order_tab_1`(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);")).WithArgs(1, int64(1), "1", 1.0).WillReturnError(newMockErr("db"))
				s.mock02.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_1`.`order_tab_0`(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);")).WithArgs(3, int64(3), "3", 3.0).WillReturnError(newMockErr("db"))
				s.mock01.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_0`.`order_tab_2`(`user_id`,`order_id`,`content`,`account`) VALUES(?,?,?,?);")).WithArgs(2, int64(2), "2", 2.0).WillReturnError(newMockErr("db"))
			},
			wantErr: multierr.Combine(newMockErr("db"), newMockErr("db"), newMockErr("db")),
		},
	}
	for _, tc := range testcases {
		s.T().Run(tc.name, func(t *testing.T) {
			tc.mockDb()
			res := tc.si.Exec(context.Background())
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

func TestShardingInsertSuite(t *testing.T) {
	suite.Run(t, &ShardingInsertSuite{})
}

func MasterSlavesMockDB(db *sql.DB) *masterslave.MasterSlavesDB {
	return masterslave.NewMasterSlavesDB(db)
}
