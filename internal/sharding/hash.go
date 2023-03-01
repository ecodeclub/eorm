package sharding

import (
	"context"
)

type Hash struct {
	Datasource   string
	Base         int
	ShardingKey  string
	DBPattern    string
	TablePattern string
}

func (h Hash) Broadcast(ctx context.Context) []Dst {
	panic("implement me")
}

func (h Hash) Sharding(ctx context.Context, req Request) (Result, error) {
	panic("implement me")
}

func (h Hash) ShardingKeys() []string {
	return []string{h.ShardingKey}
}
