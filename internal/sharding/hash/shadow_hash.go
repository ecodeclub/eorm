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
	"strings"

	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/sharding"
)

// ShadowHash TODO experiemntal
type ShadowHash struct {
	*Hash
	Prefix string
}

func (h *ShadowHash) Broadcast(ctx context.Context) []sharding.Dst {
	res := make([]sharding.Dst, 0, 8)
	for i := 0; i < h.DBPattern.Base; i++ {
		dbName := fmt.Sprintf(h.Prefix+h.DBPattern.Name, i)
		for j := 0; j < h.TablePattern.Base; j++ {
			res = append(res, sharding.Dst{
				Name:  h.DsPattern.Name,
				DB:    dbName,
				Table: fmt.Sprintf(h.Prefix+h.TablePattern.Name, j),
			})
		}
	}
	return res
}

func (h *ShadowHash) Sharding(ctx context.Context, req sharding.Request) (sharding.Response, error) {
	if h.ShardingKey == "" {
		return sharding.EmptyResp, errs.ErrMissingShardingKey
	}
	skVal, ok := req.SkValues[h.ShardingKey]
	if !ok {
		return sharding.Response{Dsts: h.Broadcast(ctx)}, nil
	}
	dbName := h.DBPattern.Name
	if !h.DBPattern.NotSharding && strings.Contains(dbName, "%d") {
		dbName = fmt.Sprintf(dbName, skVal.(int)%h.DBPattern.Base)
	}
	tbName := h.TablePattern.Name
	if !h.TablePattern.NotSharding && strings.Contains(tbName, "%d") {
		tbName = fmt.Sprintf(tbName, skVal.(int)%h.TablePattern.Base)
	}
	dsName := h.DsPattern.Name
	if !h.DsPattern.NotSharding && strings.Contains(dsName, "%d") {
		dsName = fmt.Sprintf(dsName, skVal.(int)%h.DsPattern.Base)
	}
	if isSourceKey(ctx) {
		dsName = h.Prefix + dsName
	}
	if isDBKey(ctx) {
		dbName = h.Prefix + dbName
	}

	if isTableKey(ctx) {
		tbName = h.Prefix + tbName
	}

	return sharding.Response{
		Dsts: []sharding.Dst{{Name: dsName, DB: dbName, Table: tbName}},
	}, nil
}

type sourceKey struct{}

type dbKey struct{}

type tableKey struct{}

func CtxWithTableKey(ctx context.Context) context.Context {
	return context.WithValue(ctx, tableKey{}, true)
}

func CtxWithDBKey(ctx context.Context) context.Context {
	return context.WithValue(ctx, dbKey{}, true)
}

func CtxWithSourceKey(ctx context.Context) context.Context {
	return context.WithValue(ctx, sourceKey{}, true)
}

func isSourceKey(ctx context.Context) bool {
	return ctx.Value(sourceKey{}) != nil
}

func isDBKey(ctx context.Context) bool {
	return ctx.Value(dbKey{}) != nil
}

func isTableKey(ctx context.Context) bool {
	return ctx.Value(tableKey{}) != nil
}
