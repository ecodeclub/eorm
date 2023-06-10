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
	"github.com/ecodeclub/eorm/internal/errs"
	"go.uber.org/multierr"
)

type DelayTxFactory struct{}

func (_ DelayTxFactory) TxOf(ctx Context, beginners map[string]datasource.TxBeginner) (datasource.Tx, error) {
	return NewDelayTx(ctx, beginners), nil
}

type DelayTx struct {
	ctx       Context
	lock      sync.RWMutex
	txs       map[string]datasource.Tx
	beginners map[string]datasource.TxBeginner
}

func (t *DelayTx) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	// 防止 GetMulti 的查询重复创建多个事务
	t.lock.RLock()
	tx, ok := t.txs[query.DB]
	t.lock.RUnlock()
	if ok {
		return tx.Query(ctx, query)
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	if tx, ok = t.txs[query.DB]; ok {
		return tx.Query(ctx, query)
	}
	db, ok := t.beginners[query.DB]
	if !ok {
		return nil, errs.ErrNotFoundTargetDB
	}
	tx, err := db.BeginTx(t.ctx.TxCtx, t.ctx.Opts)
	if err != nil {
		return nil, err
	}
	t.txs[query.DB] = tx
	return tx.Query(ctx, query)
}

func (t *DelayTx) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	// 防止 GetMulti 的查询重复创建多个事务
	t.lock.RLock()
	tx, ok := t.txs[query.DB]
	t.lock.RUnlock()
	if ok {
		return tx.Exec(ctx, query)
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	if tx, ok = t.txs[query.DB]; ok {
		return tx.Exec(ctx, query)
	}
	db, ok := t.beginners[query.DB]
	if !ok {
		return nil, errs.ErrNotFoundTargetDB
	}
	tx, err := db.BeginTx(t.ctx.TxCtx, t.ctx.Opts)
	if err != nil {
		return nil, err
	}
	t.txs[query.DB] = tx
	return tx.Exec(ctx, query)
}

func (t *DelayTx) Commit() error {
	var err error
	for name, tx := range t.txs {
		if er := tx.Commit(); er != nil {
			err = multierr.Combine(
				err, fmt.Errorf("masterslave DB name [%s] error: %w", name, er))
		}
	}
	return err
}

func (t *DelayTx) Rollback() error {
	var err error
	for name, tx := range t.txs {
		if er := tx.Rollback(); er != nil {
			err = multierr.Combine(
				err, fmt.Errorf("masterslave DB name [%s] error: %w", name, er))
		}
	}
	return err
}

func NewDelayTx(ctx Context, beginners map[string]datasource.TxBeginner) *DelayTx {
	return &DelayTx{ctx: ctx, beginners: beginners}
}
