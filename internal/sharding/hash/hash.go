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

package hash

import (
	"context"
	"fmt"

	"github.com/ecodeclub/eorm/internal/errs"
	operator "github.com/ecodeclub/eorm/internal/operator"
	"github.com/ecodeclub/eorm/internal/sharding"
)

// Hash TODO experiemntal
type Hash struct {
	// Base         int
	ShardingKey  string
	DBPattern    *Pattern
	TablePattern *Pattern
	// Datasource Pattern
	DsPattern *Pattern
}

func (h *Hash) Broadcast(ctx context.Context) []sharding.Dst {
	if !h.DBPattern.NotSharding && h.TablePattern.NotSharding && h.DsPattern.NotSharding { // 只分库
		return h.onlyDBroadcast(ctx)
	} else if h.DBPattern.NotSharding && !h.TablePattern.NotSharding && h.DsPattern.NotSharding { // 只分表
		return h.onlyTableBroadcast(ctx)
	} else if h.DBPattern.NotSharding && h.TablePattern.NotSharding && !h.DsPattern.NotSharding { // 只分集群
		return h.onlyDataSourceBroadcast(ctx)
	} else if !h.DBPattern.NotSharding && !h.TablePattern.NotSharding && !h.DsPattern.NotSharding { // 分集群分库分表
		return h.allBroadcast(ctx)
	}
	// 分库分表
	return h.defaultBroadcast(ctx)
}

func (h *Hash) defaultBroadcast(ctx context.Context) []sharding.Dst {
	res := make([]sharding.Dst, 0, 8)
	for i := 0; i < h.DBPattern.Base; i++ {
		dbName := fmt.Sprintf(h.DBPattern.Name, i)
		for j := 0; j < h.TablePattern.Base; j++ {
			res = append(res, sharding.Dst{
				Name:  h.DsPattern.Name,
				DB:    dbName,
				Table: fmt.Sprintf(h.TablePattern.Name, j),
			})
		}
	}
	return res
}

func (h *Hash) allBroadcast(ctx context.Context) []sharding.Dst {
	res := make([]sharding.Dst, 0, 8)
	for s := 0; s < h.DsPattern.Base; s++ {
		dsName := fmt.Sprintf(h.DsPattern.Name, s)
		for i := 0; i < h.DBPattern.Base; i++ {
			dbName := fmt.Sprintf(h.DBPattern.Name, i)
			for j := 0; j < h.TablePattern.Base; j++ {
				res = append(res, sharding.Dst{
					Name: dsName, DB: dbName,
					Table: fmt.Sprintf(h.TablePattern.Name, j),
				})
			}
		}
	}
	return res
}

func (h *Hash) onlyDBroadcast(ctx context.Context) []sharding.Dst {
	res := make([]sharding.Dst, 0, 8)
	for i := 0; i < h.DBPattern.Base; i++ {
		res = append(res, sharding.Dst{
			Name:  h.DsPattern.Name,
			Table: h.TablePattern.Name,
			DB:    fmt.Sprintf(h.DBPattern.Name, i),
		})
	}
	return res
}

func (h *Hash) onlyTableBroadcast(ctx context.Context) []sharding.Dst {
	res := make([]sharding.Dst, 0, 8)
	for j := 0; j < h.TablePattern.Base; j++ {
		res = append(res, sharding.Dst{
			Name:  h.DsPattern.Name,
			DB:    h.DBPattern.Name,
			Table: fmt.Sprintf(h.TablePattern.Name, j),
		})
	}
	return res
}

func (h *Hash) onlyDataSourceBroadcast(ctx context.Context) []sharding.Dst {
	res := make([]sharding.Dst, 0, 8)
	for j := 0; j < h.DsPattern.Base; j++ {
		res = append(res, sharding.Dst{
			Name:  fmt.Sprintf(h.DsPattern.Name, j),
			DB:    h.DBPattern.Name,
			Table: h.TablePattern.Name,
		})
	}
	return res
}

func (h *Hash) Sharding(ctx context.Context, req sharding.Request) (sharding.Response, error) {
	if h.ShardingKey == "" {
		return sharding.EmptyResp, errs.ErrMissingShardingKey
	}
	skVal, ok := req.SkValues[h.ShardingKey]
	if !ok {
		return sharding.Response{Dsts: h.Broadcast(ctx)}, nil
	}
	switch req.Op {
	case operator.OpEQ:
		dbName := h.DBPattern.Name
		if !h.DBPattern.NotSharding {
			dbName = fmt.Sprintf(dbName, skVal.(int)%h.DBPattern.Base)
		}
		tbName := h.TablePattern.Name
		if !h.TablePattern.NotSharding {
			tbName = fmt.Sprintf(tbName, skVal.(int)%h.TablePattern.Base)
		}
		dsName := h.DsPattern.Name
		if !h.DsPattern.NotSharding {
			dsName = fmt.Sprintf(dsName, skVal.(int)%h.DsPattern.Base)
		}
		return sharding.Response{
			Dsts: []sharding.Dst{{Name: dsName, DB: dbName, Table: tbName}},
		}, nil
	case operator.OpGT, operator.OpLT, operator.OpGTEQ,
		operator.OpLTEQ, operator.OpNEQ, operator.OpNotIN:
		return sharding.Response{Dsts: h.Broadcast(ctx)}, nil
	default:
		return sharding.EmptyResp, errs.NewUnsupportedOperatorError(req.Op.Text)
	}
}

func (h *Hash) ShardingKeys() []string {
	return []string{h.ShardingKey}
}

type Pattern struct {
	Base        int
	Name        string
	NotSharding bool
}
