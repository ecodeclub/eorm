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

// +build e2e

package integration_test

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gotomicro/eorm"
	"github.com/gotomicro/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

type InsertTestSuite struct {
	suite.Suite
	driver string
	dsn string
	orm *eorm.Orm
}

func (i *InsertTestSuite) SetupSuite() {
	t := i.T()
	orm, err := eorm.Open(i.driver, i.dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = orm.Wait(); err != nil {
		t.Fatal(err)
	}
	i.orm = orm
}

func (i *InsertTestSuite) TearDownTest() {
	_, err := eorm.NewQuerier(i.orm, "DELETE FROM `simple_struct`").Exec(context.Background())
	if err != nil {
		i.T().Fatal(err)
	}
}

func (i *InsertTestSuite) TestInsert() {
	testCases := []struct{
		name string
		exec Exec
		rowsAffected int64
		wantErr error
	} {
		{
			name: "id only",
			exec: eorm.NewInserter[test.SimpleStruct](i.orm).Values(&test.SimpleStruct{Id: 1}),
			rowsAffected: 1,
		},
	}
	for _, tc := range testCases {
		i.T().Run(tc.name, func(t *testing.T) {
			res, err := tc.exec.Exec(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			affected, err := res.RowsAffected()
			assert.Equal(t, tc.rowsAffected, affected)
		})
	}
}

// Exec 暂时作为测试的接口
// TODO 挪过去 eorm
type Exec interface {
	Exec(ctx context.Context) (sql.Result, error)
}

func TestMySQL8Insert(t *testing.T) {
	suite.Run(t, &InsertTestSuite{
		driver: "mysql",
		dsn: "root:root@tcp(localhost:13306)/integration_test",
	})
}
