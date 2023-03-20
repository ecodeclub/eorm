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

package datasource

import (
	"context"
	"database/sql"

	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/query"
)

var _ DataSource = &ShardingDataSource{}

type ShardingDataSource struct {
	sources map[string]DataSource
}

func (s *ShardingDataSource) Query(ctx context.Context, query query.Query) (*sql.Rows, error) {
	ds, ok := s.sources[query.Datasource]
	if !ok {
		return nil, errs.ErrNotFoundTargetDataSource
	}
	return ds.Query(ctx, query)
}

func (s *ShardingDataSource) Exec(ctx context.Context, query query.Query) (sql.Result, error) {
	ds, ok := s.sources[query.Datasource]
	if !ok {
		return nil, errs.ErrNotFoundTargetDataSource
	}
	return ds.Exec(ctx, query)
}

//func (s *ShardingDataSource) BeginTx(ctx context.Context, query *sharding.Query, opts *sql.TxOptions) (*transaction.Tx, error) {
//	ds, ok := s.sources[query.Datasource]
//	if !ok {
//		return nil, errs.ErrNotFoundTargetDataSource
//	}
//	tx, err := db.db.BeginTx(ctx, opts)
//	if err != nil {
//		return nil, err
//	}
//	return &Tx{tx: tx, db: db.db, Core: db.Core}, nil
//}

func NewShardingDataSource(m map[string]DataSource) DataSource {
	return &ShardingDataSource{
		sources: m,
	}
}
