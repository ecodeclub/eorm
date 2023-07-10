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

package shardingsource

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ecodeclub/eorm/internal/datasource/transaction"

	"github.com/ecodeclub/eorm/internal/datasource"
	"go.uber.org/multierr"

	"github.com/ecodeclub/eorm/internal/errs"
)

var _ datasource.TxBeginner = &ShardingDataSource{}
var _ datasource.DataSource = &ShardingDataSource{}
var _ datasource.Finder = &ShardingDataSource{}

type ShardingDataSource struct {
	sources map[string]datasource.DataSource
}

func (s *ShardingDataSource) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	ds, err := s.getTgt(query)
	if err != nil {
		return nil, err
	}
	return ds.Query(ctx, query)
}

func (s *ShardingDataSource) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	ds, err := s.getTgt(query)
	if err != nil {
		return nil, err
	}
	return ds.Exec(ctx, query)
}

func (s *ShardingDataSource) FindTgt(ctx context.Context, query datasource.Query) (datasource.TxBeginner, error) {
	ds, err := s.getTgt(query)
	if err != nil {
		return nil, err
	}
	f, ok := ds.(datasource.Finder)
	if !ok {
		return nil, errs.NewErrNotCompleteFinder(query.Datasource)
	}
	return f.FindTgt(ctx, query)
}

func (s *ShardingDataSource) getTgt(query datasource.Query) (datasource.DataSource, error) {
	ds, ok := s.sources[query.Datasource]
	if !ok {
		return nil, errs.NewErrNotFoundTargetDataSource(query.Datasource)
	}
	return ds, nil
}

func (s *ShardingDataSource) BeginTx(ctx context.Context, opts *sql.TxOptions) (datasource.Tx, error) {
	facade, err := transaction.NewTxFacade(ctx, s)
	if err != nil {
		return nil, err
	}
	return facade.BeginTx(ctx, opts)
}

func NewShardingDataSource(m map[string]datasource.DataSource) datasource.DataSource {
	return &ShardingDataSource{
		sources: m,
	}
}

func (s *ShardingDataSource) Close() error {
	var err error
	for name, inst := range s.sources {
		if er := inst.Close(); er != nil {
			err = multierr.Combine(
				err, fmt.Errorf("source name [%s] error: %w", name, er))
		}
	}
	return err
}
