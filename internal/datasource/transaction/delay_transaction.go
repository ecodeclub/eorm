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
	"fmt"
	"sync"

	"github.com/ecodeclub/eorm/internal/datasource"
	"go.uber.org/multierr"
)

type DelayTxFactory struct{}

func (DelayTxFactory) TxOf(ctx Context, finder datasource.Finder) (datasource.Tx, error) {
	return NewDelayTx(ctx, finder), nil
}

type DelayTx struct {
	ctx    Context
	lock   sync.RWMutex
	txs    map[string]datasource.Tx
	finder datasource.Finder
}

func (t *DelayTx) findTgt(ctx context.Context, query datasource.Query) (datasource.TxBeginner, error) {
	return t.finder.FindTgt(ctx, query)
}

func (t *DelayTx) findOrBeginTx(ctx context.Context, query datasource.Query) (datasource.Tx, error) {
	t.lock.RLock()
	tx, ok := t.txs[query.DB]
	t.lock.RUnlock()
	if ok {
		return tx, nil
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	if tx, ok = t.txs[query.DB]; ok {
		return tx, nil
	}
	var err error
	db, err := t.findTgt(ctx, query)
	if err != nil {
		return nil, err
	}
	tx, err = db.BeginTx(t.ctx.TxCtx, t.ctx.Opts)
	if err != nil {
		return nil, err
	}
	t.txs[query.DB] = tx
	return tx, nil
}

func (t *DelayTx) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	// 防止 GetMulti 的查询重复创建多个事务
	tx, err := t.findOrBeginTx(ctx, query)
	if err != nil {
		return nil, err
	}
	return tx.Query(ctx, query)
}

func (t *DelayTx) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	tx, err := t.findOrBeginTx(ctx, query)
	if err != nil {
		return nil, err
	}
	return tx.Exec(ctx, query)
}

func (t *DelayTx) Commit() error {
	var err error
	for name, tx := range t.txs {
		if er := tx.Commit(); er != nil {
			err = multierr.Combine(
				err, fmt.Errorf("masterslave DB name [%s] Commit error: %w", name, er))
		}
	}
	return err
}

func (t *DelayTx) Rollback() error {
	var err error
	for name, tx := range t.txs {
		if er := tx.Rollback(); er != nil {
			err = multierr.Combine(
				err, fmt.Errorf("masterslave DB name [%s] Rollback error: %w", name, er))
		}
	}
	return err
}

func NewDelayTx(ctx Context, finder datasource.Finder) *DelayTx {
	return &DelayTx{
		ctx:    ctx,
		finder: finder,
		txs:    make(map[string]datasource.Tx, 8),
	}
}
