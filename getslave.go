package eorm

import (
	"context"
	"database/sql"
	"sync/atomic"
)

type SlaveGeter interface {
	Next(ctx context.Context) (*sql.DB, error)
}

type roundrobin struct {
	master *sql.DB
	slaves []*sql.DB
	cnt    uint32
}

func (r *roundrobin) Next(ctx context.Context) (*sql.DB, error) {
	// 当从库一个都没有时，返回主库
	if len(r.slaves) == 0 {
		return r.master, nil
	}
	cnt := atomic.AddUint32(&r.cnt, 1)
	index := int(cnt) % len(r.slaves)
	return r.slaves[index], nil
}

func Newroundrobin(master *sql.DB, slaves ...*sql.DB) *roundrobin {
	return &roundrobin{
		master: master,
		slaves: slaves,
	}
}
