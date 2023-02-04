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
)

type ShardingSelector[T any] struct {
	builder
	table   *T
	db      *ShardingDB
	where   []Predicate
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
	panic("implement me")
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
