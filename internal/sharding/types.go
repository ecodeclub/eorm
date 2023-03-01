package sharding

import "context"

type Algorithm interface {
	// Sharding 返回分库分表之后目标库和目标表信息
	Sharding(ctx context.Context, req Request) (Result, error)
	// Broadcast 返回所有的目标库、目标表
	Broadcast(ctx context.Context) []Dst
	// ShardingKeys 返回所有的 sharding key
	// 这部分不包含任何放在 context.Context 中的部分，例如 shadow 标记位等
	// 或者说，它只是指数据库中用于分库分表的列
	ShardingKeys() []string
}

type Result struct {
	Dsts []Dst
}

type Dst struct {
	Name  string
	DB    string
	Table string
}

type Request struct {
	SkValues map[string]any
}
