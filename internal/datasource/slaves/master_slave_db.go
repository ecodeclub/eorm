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

package slaves

import (
	"context"
	"database/sql"

	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/transaction"
	"github.com/ecodeclub/eorm/internal/query"
)

var _ datasource.DataSource = &MasterSlavesDB{}

type MasterSlavesDB struct {
	master *sql.DB
	slaves Slaves
}

type key string

const (
	master key = "master"
)

func (m *MasterSlavesDB) Query(ctx context.Context, query query.Query) (*sql.Rows, error) {
	_, ok := ctx.Value(master).(bool)
	if ok {
		return m.master.QueryContext(ctx, query.SQL, query.Args...)
	}
	slave, err := m.slaves.Next(ctx)
	if err != nil {
		return nil, err
	}
	return slave.DB.QueryContext(ctx, query.SQL, query.Args...)
}

func (m *MasterSlavesDB) Exec(ctx context.Context, query query.Query) (sql.Result, error) {
	return m.master.ExecContext(ctx, query.SQL, query.Args...)
}

func (m *MasterSlavesDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*transaction.Tx, error) {
	tx, err := m.master.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return transaction.BeginTx(tx, m), nil
}

func NewMasterSlaveDB(master *sql.DB, opts ...MasterSlaveDBOption) *MasterSlavesDB {
	db := &MasterSlavesDB{
		master: master,
	}
	for _, opt := range opts {
		opt(db)
	}
	return db
}

type MasterSlaveDBOption func(db *MasterSlavesDB)

func MasterSlaveWithSlaves(s Slaves) MasterSlaveDBOption {
	return func(db *MasterSlavesDB) {
		db.slaves = s
	}
}

func UseMaster(ctx context.Context) context.Context {
	return context.WithValue(ctx, master, true)
}

// MasterSlaveMemoryDB 返回一个基于内存的 MasterSlaveDB，它使用的是 sqlite3 内存模式。
func MasterSlaveMemoryDB() *MasterSlavesDB {
	db, err := sql.Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	if err != nil {
		panic(err)
	}
	masterSlaveDB := NewMasterSlaveDB(db)
	return masterSlaveDB
}
