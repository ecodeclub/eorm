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

	"github.com/ecodeclub/eorm/internal/datasource"

	"github.com/ecodeclub/eorm/internal/errs"
)

var _ datasource.DataSource = &ShardingDataSource{}

type ShardingDataSource struct {
	sources map[string]datasource.DataSource
}

func (s *ShardingDataSource) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	ds, ok := s.sources[query.Datasource]
	if !ok {
		return nil, errs.ErrNotFoundTargetDataSource
	}
	return ds.Query(ctx, query)
}

func (s *ShardingDataSource) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	ds, ok := s.sources[query.Datasource]
	if !ok {
		return nil, errs.ErrNotFoundTargetDataSource
	}
	return ds.Exec(ctx, query)
}

func NewShardingDataSource(m map[string]datasource.DataSource) datasource.DataSource {
	return &ShardingDataSource{
		sources: m,
	}
}
