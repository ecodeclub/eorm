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

package masterslave

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"
	"go.uber.org/multierr"

	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/transaction"
)

var _ datasource.TxBeginner = &MasterSlavesDB{}
var _ datasource.DataSource = &MasterSlavesDB{}

type MasterSlavesDB struct {
	master *sql.DB
	slaves slaves.Slaves
}

type key string

const (
	master key = "master"
)

func (m *MasterSlavesDB) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
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

func (m *MasterSlavesDB) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	return m.master.ExecContext(ctx, query.SQL, query.Args...)
}

func (m *MasterSlavesDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (datasource.Tx, error) {
	tx, err := m.master.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return transaction.NewTx(tx, m), nil
}

func NewMasterSlavesDB(master *sql.DB, opts ...MasterSlavesDBOption) *MasterSlavesDB {
	db := &MasterSlavesDB{
		master: master,
	}
	for _, opt := range opts {
		opt(db)
	}
	return db
}

func (m *MasterSlavesDB) Close() error {
	var err error
	if er := m.master.Close(); er != nil {
		err = multierr.Combine(
			err, fmt.Errorf("master error: %w", er))
	}
	if m.slaves != nil {
		if er := m.slaves.Close(); er != nil {
			err = multierr.Combine(err, er)
		}
	}
	return err
}

type MasterSlavesDBOption func(db *MasterSlavesDB)

func MasterSlavesWithSlaves(s slaves.Slaves) MasterSlavesDBOption {
	return func(db *MasterSlavesDB) {
		db.slaves = s
	}
}

func UseMaster(ctx context.Context) context.Context {
	return context.WithValue(ctx, master, true)
}
