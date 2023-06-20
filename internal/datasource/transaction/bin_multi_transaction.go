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

	"github.com/ecodeclub/eorm/internal/datasource"
)

type BinMultiTxFactory struct{}

func (_ BinMultiTxFactory) TxOf(ctx Context, finder datasource.Finder) (datasource.Tx, error) {
	return NewBinMultiTx(ctx, finder), nil
}

type BinMultiTx struct {
	DB     string
	ctx    Context
	lock   sync.RWMutex
	tx     datasource.Tx
	finder datasource.Finder
}

func (t *BinMultiTx) findTgt(ctx context.Context, query datasource.Query) (datasource.TxBeginner, error) {
	return t.finder.FindTgt(ctx, query)
}

func (t *BinMultiTx) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	t.lock.RLock()
	if t.DB != "" && t.tx != nil {
		return t.tx.Query(ctx, query)
	}
	t.lock.RUnlock()

	t.lock.Lock()
	defer t.lock.Unlock()
	if t.DB != "" && t.tx != nil {
		return t.tx.Query(ctx, query)
	}
	var err error
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
	return tx.Query(ctx, query)
}

func (t *BinMultiTx) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	// 防止 GetMulti 的查询重复创建多个事务
	t.lock.RLock()
	if t.DB != "" && t.tx != nil {
		return t.tx.Exec(ctx, query)
	}
	t.lock.RUnlock()

	t.lock.Lock()
	defer t.lock.Unlock()
	if t.DB != "" && t.tx != nil {
		return t.tx.Exec(ctx, query)
	}
	var err error
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
	return tx.Exec(ctx, query)
}

func (t *BinMultiTx) Commit() error {
	if t.tx != nil {
		return t.tx.Commit()
	}
	return nil
}

func (t *BinMultiTx) Rollback() error {
	if t.tx != nil {
		return t.tx.Rollback()
	}
	return nil
}

func NewBinMultiTx(ctx Context, finder datasource.Finder) *BinMultiTx {
	return &BinMultiTx{
		ctx:    ctx,
		finder: finder,
	}
}
