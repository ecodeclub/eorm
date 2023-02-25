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

	"github.com/ecodeclub/eorm/internal/dialect"
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/model"
	"github.com/ecodeclub/eorm/internal/valuer"
)

type ShardingQuery struct {
	SQL  string
	Args []any
	DB   string
}

type Dst struct {
	DB    string
	Table string
}

type ShardingAlgorithm interface {
	Broadcast() []Dst
}

type dbNameKey struct{}

type ShardingDBOption func(db *ShardingDB)

func CtxWithDBName(ctx context.Context, dbName string) context.Context {
	return context.WithValue(ctx, dbNameKey{}, dbName)
}

func ctxGetDBName(ctx context.Context) (string, error) {
	val := ctx.Value(dbNameKey{})
	dbName, ok := val.(string)
	if !ok {
		return "", errs.ErrCtxGetDBName
	}
	return dbName, nil
}

type ShardingDB struct {
	core
	DBs    map[string]*MasterSlavesDB
	Tables map[string]bool
}

func OpenShardingDB(driver string, DBs map[string]*MasterSlavesDB, opts ...ShardingDBOption) (*ShardingDB, error) {
	dl, err := dialect.Of(driver)
	if err != nil {
		return nil, err
	}
	orm := &ShardingDB{
		DBs: DBs,
		core: core{
			metaRegistry: model.NewMetaRegistry(),
			dialect:      dl,
			valCreator: valuer.BasicTypeCreator{
				Creator: valuer.NewUnsafeValue,
			},
		},
	}
	for _, o := range opts {
		o(orm)
	}
	return orm, nil
}

func ShardingDBOptionWithMetaRegistry(r model.MetaRegistry) ShardingDBOption {
	return func(db *ShardingDB) {
		db.metaRegistry = r
	}
}

func ShardingDBOptionWithTables(m map[string]bool) ShardingDBOption {
	return func(db *ShardingDB) {
		db.Tables = m
	}
}

func (s *ShardingDB) getCore() core {
	return s.core
}

func (s *ShardingDB) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	sess, err := s.getMasterSlavesDB(ctx)
	if err != nil {
		return nil, err
	}
	return sess.queryContext(ctx, query, args...)
}

func (s *ShardingDB) execContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	sess, err := s.getMasterSlavesDB(ctx)
	if err != nil {
		return nil, err
	}
	return sess.execContext(ctx, query, args...)
}

func (s *ShardingDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	sess, err := s.getMasterSlavesDB(ctx)
	if err != nil {
		return nil, err
	}
	return sess.BeginTx(ctx, opts)
}

func (s *ShardingDB) getMasterSlavesDB(ctx context.Context) (*MasterSlavesDB, error) {
	dbName, err := ctxGetDBName(ctx)
	if err != nil {
		return nil, err
	}
	sess, ok := s.DBs[dbName]
	if !ok {
		return nil, errs.ErrNotFoundTargetDB
	}
	return sess, nil
}

func (s *ShardingDB) broadcast() []Dst {
	dsts := make([]Dst, 0, 8)
	for dbName := range s.DBs {
		for tbName := range s.Tables {
			dst := Dst{
				DB:    dbName,
				Table: tbName,
			}
			dsts = append(dsts, dst)
		}
	}
	return dsts
}
