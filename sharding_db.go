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
	"github.com/ecodeclub/eorm/internal/model"
	"github.com/ecodeclub/eorm/internal/sharding"
	"github.com/ecodeclub/eorm/internal/valuer"
)

type ShardingDBOption func(db *ShardingDB)

type ShardingDB struct {
	core
	sharding.DataSource
}

func OpenShardingDB(driver string, ds sharding.DataSource, opts ...ShardingDBOption) (*ShardingDB, error) {
	dl, err := dialect.Of(driver)
	if err != nil {
		return nil, err
	}
	orm := &ShardingDB{
		core: core{
			metaRegistry: model.NewMetaRegistry(),
			dialect:      dl,
			valCreator: valuer.BasicTypeCreator{
				Creator: valuer.NewUnsafeValue,
			},
		},
		DataSource: ds,
	}
	for _, opt := range opts {
		opt(orm)
	}
	return orm, nil
}

func ShardingDBOptionWithMetaRegistry(r model.MetaRegistry) ShardingDBOption {
	return func(db *ShardingDB) {
		db.metaRegistry = r
	}
}

func (s *ShardingDB) getCore() core {
	return s.core
}

func (s *ShardingDB) queryContext(ctx context.Context, query *sharding.Query) (*sql.Rows, error) {
	return s.Query(ctx, query)
}

func (s *ShardingDB) execContext(ctx context.Context, query *sharding.Query) (sql.Result, error) {
	return s.Exec(ctx, query)
}
