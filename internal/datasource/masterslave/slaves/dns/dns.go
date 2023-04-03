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

package dns

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/multierr"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves/dns/mysql"

	"github.com/ecodeclub/eorm/internal/errs"
	_ "github.com/go-sql-driver/mysql"
)

type Dsn interface {
	// Init 利用 dsn 来初始化本实例
	Init(dsn string) error
	// FormatByIp 使用 ip 来取代当前的域名，返回 dsn
	FormatByIp(ip string) (dsn string, err error)

	// getter 类方法，用于查询具体的 Dsn 里面的字段

	// Domain 返回 Dsn 中的域名部分
	Domain() string
}

type netResolver interface {
	LookupHost(ctx context.Context, domain string) ([]string, error)
}

var _ netResolver = (*net.Resolver)(nil)

type Slaves struct {
	resolver netResolver
	// 域名
	domain   string
	slaves   []slaves.Slave
	slaveDsn []string
	closeCh  chan struct{}
	dsn      Dsn
	cnt      uint32
	once     sync.Once
	driver   string
	interval time.Duration
	mu       sync.RWMutex
	timeout  time.Duration
}

func (s *Slaves) Next(ctx context.Context) (slaves.Slave, error) {
	if ctx.Err() != nil {
		return slaves.Slave{}, ctx.Err()
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.slaves) == 0 {
		return slaves.Slave{}, errs.ErrSlaveNotFound
	}
	cnt := atomic.AddUint32(&s.cnt, 1)
	index := int(cnt) % len(s.slaves)
	return s.slaves[index], nil
}

type SlaveOption func(s *Slaves)

// WithDSN 指定 Dsn 的实现
func WithDSN(pdsn Dsn) SlaveOption {
	return func(s *Slaves) {
		s.dsn = pdsn
	}
}

// WithDriver 指定 driver
func WithDriver(driver string) SlaveOption {
	return func(s *Slaves) {
		s.driver = driver
	}
}

// WithTimeout 指定查询 DNS 服务器的超时时间
func WithTimeout(timeout time.Duration) SlaveOption {
	return func(s *Slaves) {
		s.timeout = timeout
	}
}

// WithInterval 指定轮询 DNS 服务器的间隔
func WithInterval(interval time.Duration) SlaveOption {
	return func(s *Slaves) {
		s.interval = interval
	}
}

func withResolver(resolver netResolver) SlaveOption {
	return func(s *Slaves) {
		s.resolver = resolver
	}
}

func NewSlaves(dsn string, opts ...SlaveOption) (*Slaves, error) {
	s := &Slaves{
		closeCh:  make(chan struct{}),
		dsn:      &mysql.Dsn{},
		resolver: net.DefaultResolver,
		driver:   "mysql",
		interval: time.Second,
		timeout:  time.Second,
	}
	for _, opt := range opts {
		opt(s)
	}
	err := s.dsn.Init(dsn)
	if err != nil {
		return nil, err
	}
	s.domain = s.dsn.Domain()
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	err = s.getSlaves(ctx)
	cancel()
	if err != nil {
		return nil, err
	}
	go func() {
		ticker := time.NewTicker(s.interval)
		for {
			select {
			case <-ticker.C:
				ctx, cancel = context.WithTimeout(context.Background(), s.timeout)
				err = s.getSlaves(ctx)
				cancel()
				// 尽最大努力重试，拿到dns的响应
				if err != nil {
					log.Println(errs.NewFailedToGetSlavesFromDNS(err))
					continue
				}
			case <-s.closeCh:
				return
			}
		}
	}()
	return s, nil
}

func (s *Slaves) getSlaves(ctx context.Context) error {
	slavesip, err := s.resolver.LookupHost(ctx, s.domain)
	if err != nil {
		return err
	}
	ss := make([]slaves.Slave, 0, len(slavesip))
	sdnss := make([]string, 0, len(slavesip))
	for i, slaveIp := range slavesip {
		dsn, err := s.dsn.FormatByIp(slaveIp)
		if err != nil {
			return err
		}
		db, err := sql.Open(s.driver, dsn)
		if err != nil {
			return err
		}
		slave := slaves.Slave{
			SlaveName: strconv.Itoa(i),
			DB:        db,
		}
		sdnss = append(sdnss, dsn)
		ss = append(ss, slave)
	}
	s.mu.Lock()
	s.slaveDsn = sdnss
	s.slaves = ss
	s.mu.Unlock()
	return nil
}

func (s *Slaves) Close() error {
	var err error
	s.once.Do(func() {
		close(s.closeCh)
		err = s.closeDB()
	})
	return err
}

func (s *Slaves) closeDB() error {
	var err error
	for _, inst := range s.slaves {
		if er := inst.Close(); er != nil {
			err = multierr.Combine(
				err, fmt.Errorf("slave DB name [%s] error: %w", inst.SlaveName, er))
		}
	}
	return err
}
