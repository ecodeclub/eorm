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

	"github.com/ecodeclub/eorm/internal/datasource"
)

type Tx struct {
	core
	tx datasource.Tx
}

func (t *Tx) getCore() core {
	return t.core
}

func (t *Tx) queryContext(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	return t.tx.Query(ctx, query)
}

func (t *Tx) execContext(ctx context.Context, query datasource.Query) (sql.Result, error) {
	return t.tx.Exec(ctx, query)
}

func (t *Tx) Commit() error {
	return t.tx.Commit()
}

func (t *Tx) Rollback() error {
	return t.tx.Rollback()
}
