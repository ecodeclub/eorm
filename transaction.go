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

package eorm

import (
	"context"
	"database/sql"
)

// var _ session = &Tx{}
var _ session = &DB{}

// session 代表一个抽象的概念，即会话
// 暂时做成私有的，后面考虑重构，因为这个东西用户可能有点难以理解
type session interface {
	getCore() core
	queryContext(ctx context.Context, query string, args...any) (*sql.Rows, error)
	execContext(ctx context.Context, query string, args...any) (sql.Result, error)
}

type Tx struct {
	tx *sql.Tx
	db *DB
}

func (t *Tx) getCore() core {
	return t.db.core
}

func (t *Tx) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

func (t *Tx) execContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t *Tx) Commit() error {
	return t.tx.Commit()
}

func (t *Tx) Rollback() error {
	return t.tx.Rollback()
}