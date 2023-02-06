// Copyright 2021 gotomicro
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
	"errors"
	"fmt"
)

type ShardingSelector[T any] struct {
	builder
	table   *T
	db      *ShardingDB
	where   []Predicate
	having  []Predicate
	columns []Selectable

	orderBy []string
	offset  int
	limit   int
}

func (s *ShardingSelector[T]) Build() ([]*ShardingQuery, error) {
	// 初始化模型
	// 通过 where 条件查找目标 sharding key
	// build sql 语句
	// 返回结果
	panic("implement me")
}

func (s *ShardingSelector[T]) build(db, tbl string) (*ShardingQuery, error) {
	// 常规构建 sql
	panic("implement me")
}

func (s *ShardingSelector[T]) findDsts() ([]Dst, error) {
	//  通过遍历 pre 查找目标 shardingkey
	if len(s.where) > 0 {
		pre := s.where[0]
		for i := 1; i < len(s.where)-1; i++ {
			pre = pre.And(s.where[i])
		}
		return s.findDstByPredicate(pre)
	}
	if len(s.having) > 0 {

	}
	panic("implement me")
}

func (s *ShardingSelector[T]) findDstByPredicate(pre Predicate) ([]Dst, error) {
	var res []Dst
	switch pre.op {
	case opAnd, opOr:
		left, err := s.findDstByPredicate(pre.left.(Predicate))
		if err != nil {
			return nil, err
		}
		if len(left) == 0 {
			return s.findDstByPredicate(pre.right.(Predicate))
		}
		right, err := s.findDstByPredicate(pre.right.(Predicate))
		if err != nil {
			return nil, err
		}
		if len(right) == 0 {
			return left, nil
		}
		return s.merge(left, right), nil
	case opGT:

	case opLT:

	case opEQ:
		DBSf := s.meta.DBShardingFunc
		TbSf := s.meta.TableShardingFunc
		col, isCol := pre.left.(Column)
		right, isVals := pre.right.(values)
		if !isCol || !isVals {
			return nil, errors.New("too complex query, temporarily not supported")
		}
		skVal := right.data[0]
		if col.name == s.meta.ShardingKey {
			shardingDB, err := DBSf(skVal)
			if err != nil {
				return nil, errors.New("execute sharding algorithm err")
			}
			shardingTbl, err := TbSf(skVal)
			if err != nil {
				return nil, errors.New("execute sharding algorithm err")
			}
			dst := Dst{
				DB:    shardingDB,
				Table: shardingTbl,
			}
			res = append(res, dst)
		}
	default:
		return nil, fmt.Errorf("eorm: operators that do not know how to handle %v", pre.op.text)
	}
	return res, nil
}

func (s *ShardingSelector[T]) merge(left, right []Dst) []Dst {
	res := make([]Dst, 0, len(left)+len(right))
	for _, r := range right {
		exist := false
		for _, l := range left {
			if r.DB == l.DB && r.Table == l.Table {
				exist = true
			}
		}
		if exist {
			res = append(res, r)
		}
	}
	return res
}

func (s *ShardingSelector[T]) Get(ctx context.Context) (*T, error) {
	qs, err := s.Build()
	if err != nil {
		return nil, err
	}
	// 要确保前面的改写 SQL 只能生成一个 SQL ???
	if len(qs) > 1 {
		return nil, errors.New("只能生成一个 SQL")
	}
	panic("implement me")
}

func (s *ShardingSelector[T]) GetMulti(ctx context.Context) ([]*T, error) {
	panic("implement me")
}
