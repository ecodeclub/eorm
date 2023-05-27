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

	"github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/eorm/internal/errs"
	operator "github.com/ecodeclub/eorm/internal/operator"
	"github.com/ecodeclub/eorm/internal/sharding"
)

type shardingBuilder struct {
	builder
}

func (b *shardingBuilder) findDst(ctx context.Context, predicates ...Predicate) (sharding.Response, error) {
	//  通过遍历 pre 查找目标 shardingkey
	if len(predicates) > 0 {
		pre := predicates[0]
		for i := 1; i < len(predicates)-1; i++ {
			pre = pre.And(predicates[i])
		}
		return b.findDstByPredicate(ctx, pre)
	}
	res := sharding.Response{
		Dsts: b.meta.ShardingAlgorithm.Broadcast(ctx),
	}
	return res, nil
}

func (b *shardingBuilder) findDstByPredicate(ctx context.Context, pre Predicate) (sharding.Response, error) {
	switch pre.op {
	case opAnd:
		left, err := b.findDstByPredicate(ctx, pre.left.(Predicate))
		if err != nil {
			return sharding.EmptyResp, err
		}
		right, err := b.findDstByPredicate(ctx, pre.right.(Predicate))
		if err != nil {
			return sharding.EmptyResp, err
		}
		return b.mergeAnd(left, right), nil
	case opOr:
		left, err := b.findDstByPredicate(ctx, pre.left.(Predicate))
		if err != nil {
			return sharding.EmptyResp, err
		}
		right, err := b.findDstByPredicate(ctx, pre.right.(Predicate))
		if err != nil {
			return sharding.EmptyResp, err
		}
		return b.mergeOR(left, right), nil
	case opIn:
		col := pre.left.(Column)
		right := pre.right.(values)
		var results []sharding.Response
		for _, val := range right.data {
			res, err := b.meta.ShardingAlgorithm.Sharding(ctx,
				sharding.Request{Op: opEQ, SkValues: map[string]any{col.name: val}})
			if err != nil {
				return sharding.EmptyResp, err
			}
			results = append(results, res)
		}
		return b.mergeIN(results), nil
	case opNot:
		nPre, err := b.negatePredicate(pre.right.(Predicate))
		if err != nil {
			return sharding.EmptyResp, err
		}
		return b.findDstByPredicate(ctx, nPre)
	case opNotIN:
		return b.meta.ShardingAlgorithm.Sharding(ctx,
			sharding.Request{Op: opNotIN, SkValues: map[string]any{}})
	case opEQ, opGT, opLT, opGTEQ, opLTEQ, opNEQ:
		col, isCol := pre.left.(Column)
		right, isVals := pre.right.(valueExpr)
		if !isCol || !isVals {
			return sharding.EmptyResp, errs.ErrUnsupportedTooComplexQuery
		}
		return b.meta.ShardingAlgorithm.Sharding(ctx,
			sharding.Request{Op: pre.op, SkValues: map[string]any{col.name: right.val}})
	default:
		return sharding.EmptyResp, errs.NewUnsupportedOperatorError(pre.op.Text)
	}
}

func (b *shardingBuilder) negatePredicate(pre Predicate) (Predicate, error) {
	switch pre.op {
	case opAnd:
		left, err := b.negatePredicate(pre.left.(Predicate))
		if err != nil {
			return emptyPredicate, err
		}
		right, err := b.negatePredicate(pre.right.(Predicate))
		if err != nil {
			return emptyPredicate, err
		}
		return Predicate{
			left: left, op: opOr, right: right,
		}, nil
	case opOr:
		left, err := b.negatePredicate(pre.left.(Predicate))
		if err != nil {
			return emptyPredicate, err
		}
		right, err := b.negatePredicate(pre.right.(Predicate))
		if err != nil {
			return emptyPredicate, err
		}
		return Predicate{
			left: left, op: opAnd, right: right,
		}, nil
	default:
		nOp, err := operator.NegateOp(pre.op)
		if err != nil {
			return emptyPredicate, err
		}
		return Predicate{left: pre.left, op: nOp, right: pre.right}, nil
	}
}

// mergeAnd 两个分片结果的交集
func (*shardingBuilder) mergeAnd(left, right sharding.Response) sharding.Response {
	dsts := slice.IntersectSetFunc[sharding.Dst](left.Dsts, right.Dsts, func(src, dst sharding.Dst) bool {
		return src.Equals(dst)
	})
	return sharding.Response{Dsts: dsts}
}

// mergeOR 两个分片结果的并集
func (*shardingBuilder) mergeOR(left, right sharding.Response) sharding.Response {
	dsts := slice.UnionSetFunc[sharding.Dst](left.Dsts, right.Dsts, func(src, dst sharding.Dst) bool {
		return src.Equals(dst)
	})
	return sharding.Response{Dsts: dsts}
}

// mergeIN 多个分片结果的并集
func (*shardingBuilder) mergeIN(vals []sharding.Response) sharding.Response {
	var dsts []sharding.Dst
	for _, val := range vals {
		dsts = slice.UnionSetFunc[sharding.Dst](dsts, val.Dsts, func(src, dst sharding.Dst) bool {
			return src.Equals(dst)
		})
	}
	return sharding.Response{Dsts: dsts}
}
