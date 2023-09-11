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

	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/rows"
	"golang.org/x/sync/errgroup"
)

var _ Session = (*baseSession)(nil)

type baseSession struct {
	core
	executor datasource.Executor
}

func (sess *baseSession) queryContext(ctx context.Context, q Query) (rows.Rows, error) {
	return sess.executor.Query(ctx, q)
}

func (sess *baseSession) queryMulti(ctx context.Context, qs []Query) (list.List[rows.Rows], error) {
	res := &list.ConcurrentList[rows.Rows]{
		List: list.NewArrayList[rows.Rows](len(qs)),
	}
	var eg errgroup.Group
	for _, query := range qs {
		q := query
		eg.Go(func() error {
			rs, err := sess.queryContext(ctx, q)
			if err == nil {
				return res.Append(rs)
			}
			return err
		})
	}
	return res, eg.Wait()
}

func (sess *baseSession) execContext(ctx context.Context, q Query) (sql.Result, error) {
	return sess.executor.Exec(ctx, q)
}

func (sess *baseSession) getCore() core {
	return sess.core
}
