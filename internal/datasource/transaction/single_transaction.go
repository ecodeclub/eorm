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

package transaction

import (
	"context"
	"database/sql"
	"sync"

	"github.com/ecodeclub/eorm/internal/errs"

	"github.com/ecodeclub/eorm/internal/datasource"
)

type SingleTxFactory struct{}

func (SingleTxFactory) TxOf(ctx Context, finder datasource.Finder) (datasource.Tx, error) {
	return NewSingleTx(ctx, finder), nil
}

type SingleTx struct {
	DB     string
	ctx    Context
	lock   sync.RWMutex
	tx     datasource.Tx
	finder datasource.Finder
}

func (t *SingleTx) findTgt(ctx context.Context, query datasource.Query) (datasource.TxBeginner, error) {
	return t.finder.FindTgt(ctx, query)
}

func (t *SingleTx) findOrBeginTx(ctx context.Context, query datasource.Query) (datasource.Tx, error) {
	t.lock.RLock()
	if t.DB != "" && t.tx != nil {
		if t.DB != query.DB {
			t.lock.RUnlock()
			return nil, errs.NewErrDBNotEqual(t.DB, query.DB)
		}
		t.lock.RUnlock()
		return t.tx, nil
	}
	t.lock.RUnlock()
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.DB != "" && t.tx != nil {
		if t.DB != query.DB {
			return nil, errs.NewErrDBNotEqual(t.DB, query.DB)
		}
		return t.tx, nil
	}
	db, err := t.findTgt(ctx, query)
	if err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(t.ctx.TxCtx, t.ctx.Opts)
	if err != nil {
		return nil, err
	}
	t.tx = tx
	t.DB = query.DB
	return tx, nil
}

func (t *SingleTx) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	// 防止 GetMulti 的查询重复创建多个事务
	tx, err := t.findOrBeginTx(ctx, query)
	if err != nil {
		return nil, err
	}
	return tx.Query(ctx, query)
}

func (t *SingleTx) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	tx, err := t.findOrBeginTx(ctx, query)
	if err != nil {
		return nil, err
	}
	return tx.Exec(ctx, query)
}

func (t *SingleTx) Commit() error {
	if t.tx != nil {
		return t.tx.Commit()
	}
	return nil
}

func (t *SingleTx) Rollback() error {
	if t.tx != nil {
		return t.tx.Rollback()
	}
	return nil
}

func NewSingleTx(ctx Context, finder datasource.Finder) *SingleTx {
	return &SingleTx{
		ctx:    ctx,
		finder: finder,
	}
}
