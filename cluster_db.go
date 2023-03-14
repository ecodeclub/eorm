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
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/sharding"
	"sync"
)

var _ sharding.DataSource = &ClusterDB{}

type ClusterDB struct {
	MasterSlavesDBs map[string]*MasterSlavesDB
	lock            sync.Mutex
}

func (c *ClusterDB) Query(ctx context.Context, query *sharding.Query) (*sql.Rows, error) {
	ms, ok := c.MasterSlavesDBs[query.DB]
	if !ok {
		return nil, errs.ErrNotFoundTargetDB
	}
	return ms.queryContext(ctx, query.SQL, query.Args...)
}

func (c *ClusterDB) Exec(ctx context.Context, query *sharding.Query) (sql.Result, error) {
	ms, ok := c.MasterSlavesDBs[query.DB]
	if !ok {
		return nil, errs.ErrNotFoundTargetDB
	}
	return ms.execContext(ctx, query.SQL, query.Args...)
}

func (c *ClusterDB) Set(key string, db *MasterSlavesDB) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.MasterSlavesDBs[key]
	if ok {
		return errs.ErrRepeatedSetDB
	}
	c.MasterSlavesDBs[key] = db
	return nil
}

func OpenClusterDB(ms map[string]*MasterSlavesDB) *ClusterDB {
	return &ClusterDB{MasterSlavesDBs: ms}
}
