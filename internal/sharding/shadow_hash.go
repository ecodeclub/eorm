package sharding

import "context"

type ShadowHash struct {
	Hash
}

func (h ShadowHash) Sharding(ctx context.Context, req Request) (Result, error) {
	panic("implement me")
}
