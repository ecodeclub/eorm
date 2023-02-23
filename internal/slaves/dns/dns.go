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

package dns

import (
	"context"
	"database/sql"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gotomicro/eorm/internal/errs"
	"github.com/gotomicro/eorm/internal/slaves"
)

type dns interface {
	LookUpAddr(ctx context.Context, domain string) ([]string, error)
}

type dnsResolver struct {
	resolver *net.Resolver
}

func (d *dnsResolver) LookUpAddr(ctx context.Context, domain string) ([]string, error) {
	return d.resolver.LookupAddr(ctx, domain)
}

type SlaveInfo struct {
	Ip     string
	Domain string
}

type Slaves struct {
	dns dns
	// 域名
	domain   string
	slaves   []slaves.Slave
	slavedsn []string
	closech  chan struct{}
	parse    ParseDSN
	cnt      uint32
	once     sync.Once
	driver   string
	interval time.Duration
	mu       *sync.RWMutex
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

func WithParseDSN(pdsn ParseDSN) SlaveOption {
	return func(s *Slaves) {
		s.parse = pdsn
	}
}
func WithDriver(driver string) SlaveOption {
	return func(s *Slaves) {
		s.driver = driver
	}
}
func WithResolver(dns *net.Resolver) SlaveOption {
	return func(s *Slaves) {
		resolver := &dnsResolver{
			resolver: dns,
		}
		s.dns = resolver
	}
}

func withdns(d dns) SlaveOption {
	return func(s *Slaves) {
		s.dns = d
	}
}
func WithInterval(interval time.Duration) SlaveOption {
	return func(s *Slaves) {
		s.interval = interval
	}
}

func NewSlaves(dsn string, opts ...SlaveOption) (*Slaves, error) {
	slave := &Slaves{
		closech:  make(chan struct{}),
		parse:    &MysqlParse{},
		driver:   "mysql",
		interval: time.Second,
		mu:       &sync.RWMutex{},
	}
	for _, opt := range opts {
		opt(slave)
	}
	domain, err := slave.parse.Parse(dsn)
	if err != nil {
		return nil, err
	}
	slave.domain = domain
	ticker := time.NewTicker(slave.interval)
	err = slave.getSlaves(context.Background())
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			select {
			case <-ticker.C:
				err := slave.getSlaves(context.Background())
				// 错误处理还没有想好怎么搞
				if err != nil {
					continue
				}
			case <-slave.closech:
				return
			}
		}
	}()
	return slave, nil

}
func (s *Slaves) getSlaves(ctx context.Context) error {
	slavesip, err := s.dns.LookUpAddr(ctx, s.domain)
	if err != nil {
		return err
	}
	ss := make([]slaves.Slave, 0, len(slavesip))
	sdnss := make([]string, 0, len(slavesip))
	for i, slaveip := range slavesip {
		dsn, err := s.parse.Splice(slaveip)
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
	s.slavedsn = sdnss
	s.slaves = ss
	s.mu.Unlock()
	return nil
}

func (s *Slaves) getslavedsns() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.slavedsn
}

func (s *Slaves) Close() {
	s.once.Do(func() {
		close(s.closech)
	})
}
