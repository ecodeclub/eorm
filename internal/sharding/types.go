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

package sharding

import (
	"context"

	operator "github.com/ecodeclub/eorm/internal/operator"
	"github.com/ecodeclub/eorm/internal/query"
)

var EmptyResp = Response{}
var EmptyQuery = Query{}

type Algorithm interface {
	// Sharding 返回分库分表之后目标库和目标表信息
	Sharding(ctx context.Context, req Request) (Response, error)
	// Broadcast 返回所有的目标库、目标表
	Broadcast(ctx context.Context) []Dst
	// ShardingKeys 返回所有的 sharding key
	// 这部分不包含任何放在 context.Context 中的部分，例如 shadow 标记位等
	// 或者说，它只是指数据库中用于分库分表的列
	ShardingKeys() []string
}

// Executor sql 语句执行器
type Executor interface {
	Exec(ctx context.Context) Result
}

// QueryBuilder  sharding sql 构造抽象
type QueryBuilder interface {
	Build(ctx context.Context) ([]Query, error)
}

type Query = query.Query

type Dst struct {
	// Name 数据源的逻辑名字
	Name  string
	DB    string
	Table string
}

func (r Dst) Equals(l Dst) bool {
	return r.Name == l.Name && r.DB == l.DB && r.Table == l.Table
}

func (r Dst) NotEquals(l Dst) bool {
	return r.Name != l.Name || r.DB != l.DB || r.Table != l.Table
}

type Request struct {
	Op       operator.Op
	SkValues map[string]any
}

type Response struct {
	Dsts []Dst
}
