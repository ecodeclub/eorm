package sharding

import (
	"context"
	"fmt"
	"github.com/ecodeclub/eorm/internal/errs"
	"strings"
)

type Hash struct {
	Base         int
	ShardingKey  string
	DBPattern    string
	TablePattern string
	// Datasource Pattern
	DsPattern string
}

func (h Hash) Broadcast(ctx context.Context) []Dst {
	// 只分库
	if strings.Contains(h.DBPattern, "%d") && !strings.Contains(
		h.TablePattern, "%d") && !strings.Contains(h.DsPattern, "%d") {
		return h.onlyDBroadcast(ctx)

	}
	// 只分表
	if strings.Contains(h.TablePattern, "%d") && !strings.Contains(
		h.DBPattern, "%d") && !strings.Contains(h.DsPattern, "%d") {
		return h.onlyTableBroadcast(ctx)
	}
	// 分集群分库分表
	if strings.Contains(h.DBPattern, "%d") && strings.Contains(
		h.TablePattern, "%d") && strings.Contains(h.DsPattern, "%d") {
		return h.allBroadcast(ctx)

	}
	// 分库分表
	return h.defaultBroadcast(ctx)
}

func (h Hash) defaultBroadcast(ctx context.Context) []Dst {
	res := make([]Dst, 0, 8)
	for i := 1; i <= h.Base; i++ {
		DBName := fmt.Sprintf(h.DBPattern, i)
		for j := 0; j < h.Base; j++ {
			res = append(res, Dst{
				Name:  h.DsPattern,
				DB:    DBName,
				Table: fmt.Sprintf(h.TablePattern, j),
			})
		}
	}
	return res
}

func (h Hash) allBroadcast(ctx context.Context) []Dst {
	res := make([]Dst, 0, 8)
	for s := 0; s < h.Base; s++ {
		DsName := fmt.Sprintf(h.DsPattern, s)
		for i := 1; i <= h.Base; i++ {
			DBName := fmt.Sprintf(h.DBPattern, i)
			for j := 0; j < h.Base; j++ {
				res = append(res, Dst{
					Name: DsName, DB: DBName,
					Table: fmt.Sprintf(h.TablePattern, j),
				})
			}
		}
	}
	return res
}

func (h Hash) onlyDBroadcast(ctx context.Context) []Dst {
	res := make([]Dst, 0, 8)
	for i := 1; i <= h.Base; i++ {
		res = append(res, Dst{
			Name:  h.DsPattern,
			Table: h.TablePattern,
			DB:    fmt.Sprintf(h.DBPattern, i),
		})
	}
	return res
}

func (h Hash) onlyTableBroadcast(ctx context.Context) []Dst {
	res := make([]Dst, 0, 8)

	for j := 0; j < h.Base; j++ {
		res = append(res, Dst{
			Name:  h.DsPattern,
			DB:    h.DBPattern,
			Table: fmt.Sprintf(h.TablePattern, j),
		})
	}
	return res
}

func (h Hash) Sharding(ctx context.Context, req Request) (Result, error) {
	skVal, ok := req.SkValues[h.ShardingKey]
	//if !ok {
	//	return Result{
	//		Dsts: h.Broadcast(ctx),
	//	}, nil
	//}
	if !ok {
		return EmptyResult, errs.ErrMissingShardingKey
	}
	DBName := h.DBPattern
	if strings.Contains(h.DBPattern, "%d") {
		DBName = fmt.Sprintf(h.DBPattern, skVal.(int)/h.Base)
	}
	tbName := h.TablePattern
	if strings.Contains(h.TablePattern, "%d") {
		tbName = fmt.Sprintf(h.TablePattern, skVal.(int)%h.Base)
	}
	DsName := h.DsPattern
	if strings.Contains(h.DsPattern, "%d") {
		DsName = fmt.Sprintf(h.DsPattern, skVal.(int)%h.Base)
	}
	return Result{
		Dsts: []Dst{{Name: DsName, DB: DBName, Table: tbName}},
	}, nil
}

func (h Hash) ShardingKeys() []string {
	return []string{h.ShardingKey}
}
