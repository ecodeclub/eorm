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

package cluster

import (
	"context"
	"database/sql"

	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/errs"
)

var _ datasource.DataSource = &clusterDB{}

// clusterDB 以 DB 名称作为索引目标数据库
type clusterDB struct {
	// DataSource  应实现为 *masterSlavesDB
	masterSlavesDBs map[string]*masterslave.MasterSlavesDB
}

func (c *clusterDB) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	ms, ok := c.masterSlavesDBs[query.DB]
	if !ok {
		return nil, errs.ErrNotFoundTargetDB
	}
	return ms.Query(ctx, query)
}

func (c *clusterDB) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	ms, ok := c.masterSlavesDBs[query.DB]
	if !ok {
		return nil, errs.ErrNotFoundTargetDB
	}
	return ms.Exec(ctx, query)
}

//func (c *clusterDB) Set(key string, db *slaves.MasterSlavesDB) error {
//	c.lock.Lock()
//	defer c.lock.Unlock()
//	_, ok := c.MasterSlavesDBs[key]
//	if ok {
//		return errs.ErrRepeatedSetDB
//	}
//	c.MasterSlavesDBs[key] = db
//	return nil
//}

func NewClusterDB(ms map[string]*masterslave.MasterSlavesDB) datasource.DataSource {
	return &clusterDB{masterSlavesDBs: ms}
}
