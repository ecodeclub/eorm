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
		return h.onlyDBroadcast(ctx, 0, h.DBPattern.Base)
	} else if h.DBPattern.NotSharding && !h.TablePattern.NotSharding && h.DsPattern.NotSharding { // 只分表
		return h.onlyTableBroadcast(ctx, 0, h.TablePattern.Base)
	} else if h.DBPattern.NotSharding && h.TablePattern.NotSharding && !h.DsPattern.NotSharding { // 只分集群
		return h.onlyDataSourceBroadcast(ctx, 0, h.DsPattern.Base)
	} else if !h.DBPattern.NotSharding && !h.TablePattern.NotSharding && !h.DsPattern.NotSharding { // 分集群分库分表
		return h.allBroadcast(
			ctx, 0, h.DsPattern.Base, 0, h.DBPattern.Base, 0, h.TablePattern.Base)
	}
	// 分库分表
	return h.defaultBroadcast(
		ctx, 0, h.DBPattern.Base, 0, h.TablePattern.Base)
}

func (h *Hash) defaultBroadcast(ctx context.Context,
	dbStartPos, dbEndPos, tblStartBase, tblEndBase int) []sharding.Dst {
	res := make([]sharding.Dst, 0, 8)
	for i := dbStartPos; i < dbEndPos; i++ {
		dbName := fmt.Sprintf(h.DBPattern.Name, i)
		for j := tblStartBase; j < tblEndBase; j++ {
			res = append(res, sharding.Dst{
				Name:  h.DsPattern.Name,
				DB:    dbName,
				Table: fmt.Sprintf(h.TablePattern.Name, j),
			})
		}
	}
	return res
}

func (h *Hash) allBroadcast(ctx context.Context,
	dsStartPos, dsEndPos, dbStartPos, dbEndPos, tblStartBase, tblEndBase int) []sharding.Dst {
	res := make([]sharding.Dst, 0, 8)
	for s := dsStartPos; s < dsEndPos; s++ {
		dsName := fmt.Sprintf(h.DsPattern.Name, s)
		for i := dbStartPos; i < dbEndPos; i++ {
			dbName := fmt.Sprintf(h.DBPattern.Name, i)
			for j := tblStartBase; j < tblEndBase; j++ {
				res = append(res, sharding.Dst{
					Name: dsName, DB: dbName,
					Table: fmt.Sprintf(h.TablePattern.Name, j),
				})
			}
		}
	}
	return res
}

func (h *Hash) onlyDBroadcast(ctx context.Context, startPos, endPos int) []sharding.Dst {
	res := make([]sharding.Dst, 0, 8)
	for i := startPos; i < endPos; i++ {
		res = append(res, sharding.Dst{
			Name:  h.DsPattern.Name,
			Table: h.TablePattern.Name,
			DB:    fmt.Sprintf(h.DBPattern.Name, i),
		})
	}
	return res
}

func (h *Hash) onlyTableBroadcast(ctx context.Context, startPos, endPos int) []sharding.Dst {
	res := make([]sharding.Dst, 0, 8)
	for j := startPos; j < endPos; j++ {
		res = append(res, sharding.Dst{
			Name:  h.DsPattern.Name,
			DB:    h.DBPattern.Name,
			Table: fmt.Sprintf(h.TablePattern.Name, j),
		})
	}
	return res
}

func (h *Hash) onlyDataSourceBroadcast(ctx context.Context, startPos, endPos int) []sharding.Dst {
	res := make([]sharding.Dst, 0, 8)
	for j := startPos; j < endPos; j++ {
		res = append(res, sharding.Dst{
			Name:  fmt.Sprintf(h.DsPattern.Name, j),
			DB:    h.DBPattern.Name,
			Table: h.TablePattern.Name,
		})
	}
	return res
}

//func (h *Hash) Sharding(ctx context.Context, req sharding.Request) (sharding.Result, error) {
//	if h.ShardingKey == "" {
//		return sharding.EmptyResult, errs.ErrMissingShardingKey
//	}
//	skVal, ok := req.SkValues[h.ShardingKey]
//	if !ok {
//		return sharding.Result{Dsts: h.Broadcast(ctx)}, nil
//	}
//	dbName := h.DBPattern.Name
//	if !h.DBPattern.NotSharding && strings.Contains(dbName, "%d") {
//		dbName = fmt.Sprintf(dbName, skVal.(int)%h.DBPattern.Base)
//	}
//	tbName := h.TablePattern.Name
//	if !h.TablePattern.NotSharding && strings.Contains(tbName, "%d") {
//		tbName = fmt.Sprintf(tbName, skVal.(int)%h.TablePattern.Base)
//	}
//	dsName := h.DsPattern.Name
//	if !h.DsPattern.NotSharding && strings.Contains(dsName, "%d") {
//		dsName = fmt.Sprintf(dsName, skVal.(int)%h.DsPattern.Base)
//	}
//	return sharding.Result{
//		Dsts: []sharding.Dst{{Name: dsName, DB: dbName, Table: tbName}},
//	}, nil
//}

func (h *Hash) Sharding(ctx context.Context, req sharding.Request) (sharding.Result, error) {
	if h.ShardingKey == "" {
		return sharding.EmptyResult, errs.ErrMissingShardingKey
	}
	skVal, ok := req.SkValues[h.ShardingKey]
	if !ok {
		return sharding.Result{Dsts: h.Broadcast(ctx)}, nil
	}
	var dsts []sharding.Dst
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
		dsts = append(dsts, sharding.Dst{Name: dsName, DB: dbName, Table: tbName})
	case operator.OpGT:
		if !h.DBPattern.NotSharding && h.TablePattern.NotSharding && h.DsPattern.NotSharding {
			dsts = h.onlyDBroadcast(
				ctx, (skVal.(int)%h.DBPattern.Base)+1, h.DBPattern.Base)
		} else if h.DBPattern.NotSharding && !h.TablePattern.NotSharding && h.DsPattern.NotSharding {
			dsts = h.onlyTableBroadcast(
				ctx, (skVal.(int)%h.TablePattern.Base)+1, h.TablePattern.Base)
		} else if h.DBPattern.NotSharding && h.TablePattern.NotSharding && !h.DsPattern.NotSharding {
			dsts = h.onlyDataSourceBroadcast(
				ctx, (skVal.(int)%h.DsPattern.Base)+1, h.DsPattern.Base)
		} else if !h.DBPattern.NotSharding && !h.TablePattern.NotSharding && !h.DsPattern.NotSharding {
			dsts = h.allBroadcast(
				ctx, (skVal.(int)%h.DsPattern.Base)+1, h.DsPattern.Base,
				(skVal.(int)%h.DBPattern.Base)+1, h.DBPattern.Base,
				(skVal.(int)%h.TablePattern.Base)+1, h.TablePattern.Base)
		}
		dsts = h.defaultBroadcast(
			ctx, (skVal.(int)%h.DBPattern.Base)+1, h.DBPattern.Base,
			(skVal.(int)%h.TablePattern.Base)+1, h.TablePattern.Base)
	case operator.OpLT:
		if !h.DBPattern.NotSharding && h.TablePattern.NotSharding && h.DsPattern.NotSharding {
			dsts = h.onlyDBroadcast(
				ctx, 0, (skVal.(int)%h.DBPattern.Base)-1)
		} else if h.DBPattern.NotSharding && !h.TablePattern.NotSharding && h.DsPattern.NotSharding {
			dsts = h.onlyTableBroadcast(
				ctx, 0, (skVal.(int)%h.TablePattern.Base)-1)
		} else if h.DBPattern.NotSharding && h.TablePattern.NotSharding && !h.DsPattern.NotSharding {
			dsts = h.onlyDataSourceBroadcast(
				ctx, 0, (skVal.(int)%h.DsPattern.Base)-1)
		} else if !h.DBPattern.NotSharding && !h.TablePattern.NotSharding && !h.DsPattern.NotSharding {
			dsts = h.allBroadcast(
				ctx, 0, (skVal.(int)%h.DsPattern.Base)-1,
				0, (skVal.(int)%h.DBPattern.Base)-1,
				0, (skVal.(int)%h.TablePattern.Base)-1)
		}
		dsts = h.defaultBroadcast(
			ctx, 0, (skVal.(int)%h.DBPattern.Base)-1,
			0, (skVal.(int)%h.TablePattern.Base)-1)
	case operator.OpGTEQ:
		if !h.DBPattern.NotSharding && h.TablePattern.NotSharding && h.DsPattern.NotSharding {
			dsts = h.onlyDBroadcast(
				ctx, skVal.(int)%h.DBPattern.Base, h.DBPattern.Base)
		} else if h.DBPattern.NotSharding && !h.TablePattern.NotSharding && h.DsPattern.NotSharding {
			dsts = h.onlyTableBroadcast(
				ctx, skVal.(int)%h.TablePattern.Base, h.TablePattern.Base)
		} else if h.DBPattern.NotSharding && h.TablePattern.NotSharding && !h.DsPattern.NotSharding {
			dsts = h.onlyDataSourceBroadcast(
				ctx, skVal.(int)%h.DsPattern.Base, h.DsPattern.Base)
		} else if !h.DBPattern.NotSharding && !h.TablePattern.NotSharding && !h.DsPattern.NotSharding {
			dsts = h.allBroadcast(
				ctx, skVal.(int)%h.DsPattern.Base, h.DsPattern.Base,
				skVal.(int)%h.DBPattern.Base, h.DBPattern.Base,
				skVal.(int)%h.TablePattern.Base, h.TablePattern.Base)
		}
		dsts = h.defaultBroadcast(
			ctx, skVal.(int)%h.DBPattern.Base, h.DBPattern.Base,
			skVal.(int)%h.TablePattern.Base, h.TablePattern.Base)
	case operator.OpLTEQ:
		if !h.DBPattern.NotSharding && h.TablePattern.NotSharding && h.DsPattern.NotSharding {
			dsts = h.onlyDBroadcast(
				ctx, 0, skVal.(int)%h.DBPattern.Base)
		} else if h.DBPattern.NotSharding && !h.TablePattern.NotSharding && h.DsPattern.NotSharding {
			dsts = h.onlyTableBroadcast(
				ctx, 0, skVal.(int)%h.TablePattern.Base)
		} else if h.DBPattern.NotSharding && h.TablePattern.NotSharding && !h.DsPattern.NotSharding {
			dsts = h.onlyDataSourceBroadcast(
				ctx, 0, skVal.(int)%h.DsPattern.Base)
		} else if !h.DBPattern.NotSharding && !h.TablePattern.NotSharding && !h.DsPattern.NotSharding {
			dsts = h.allBroadcast(
				ctx, 0, skVal.(int)%h.DsPattern.Base,
				0, skVal.(int)%h.DBPattern.Base,
				0, skVal.(int)%h.TablePattern.Base)
		}
		dsts = h.defaultBroadcast(
			ctx, 0, skVal.(int)%h.DBPattern.Base,
			0, skVal.(int)%h.TablePattern.Base)
	default:
		return sharding.EmptyResult, errs.NewUnsupportedOperatorError(req.Op.Text)
	}
	return sharding.Result{Dsts: dsts}, nil
}

func (h *Hash) ShardingKeys() []string {
	return []string{h.ShardingKey}
}

type Pattern struct {
	Base        int
	Name        string
	NotSharding bool
}
