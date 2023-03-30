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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves/dns/mysql"

	"github.com/ecodeclub/eorm/internal/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ExampleSlaves_Close() {
	resolver := &mockResolver{
		m: map[string][]string{
			"slaves.mycompany.com": {"192.168.1.1", "192.168.1.2", "192.168.1.3"},
		},
		mu: &sync.RWMutex{},
	}
	sl, _ := NewSlaves("root:root@tcp(slaves.mycompany.com:13308)/integration_test",
		withResolver(resolver))

	err := sl.Close()
	if err == nil {
		fmt.Println("close")
	}

	// Output:
	// close
}

type mockResolver struct {
	mu *sync.RWMutex
	m  map[string][]string
}

func (m *mockResolver) LookupHost(ctx context.Context, domain string) ([]string, error) {
	if ctx.Err() != nil {
		return []string{}, ctx.Err()
	}
	m.mu.RLock()
	ips, ok := m.m[domain]
	m.mu.RUnlock()
	if !ok {
		return []string{}, fmt.Errorf("lookup %v not found", domain)
	}
	return ips, nil
}

func TestGetSlaves(t *testing.T) {
	resolver := &mockResolver{
		m: map[string][]string{
			"slaves.mycompany.com": {"192.168.1.1", "192.168.1.2", "192.168.1.3"},
		},
		mu: &sync.RWMutex{},
	}

	testcases := []struct {
		name          string
		slaves        func() (*Slaves, error)
		wantSlavednss []string
		wantErr       error
	}{
		{
			name: "normal resolver",
			slaves: func() (*Slaves, error) {
				return NewSlaves("root:root@tcp(slaves.mycompany.com:13308)/integration_test", withResolver(resolver))
			},
			wantSlavednss: []string{
				"root:root@tcp(192.168.1.1:13308)/integration_test",
				"root:root@tcp(192.168.1.2:13308)/integration_test",
				"root:root@tcp(192.168.1.3:13308)/integration_test",
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := tc.slaves()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			err = s.getSlaves(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantSlavednss, s.getSlaveDsns())
			err = s.Close()
			assert.NoError(t, err)

		})
	}
}

func TestSlaves(t *testing.T) {
	resolver := &mockResolver{
		m: map[string][]string{
			"slaves.mycompany.com": []string{
				"192.168.1.1",
			},
		},
		mu: &sync.RWMutex{},
	}
	testcases := []struct {
		name            string
		slaves          func() (*Slaves, error)
		beforeSlavednss []string
		afterSlavedns   []string
		wantErr         error
		cs              func()
	}{
		{
			name: "get slaves by resolver",
			slaves: func() (*Slaves, error) {
				s, err := NewSlaves("root:root@tcp(slaves.mycompany.com:13308)/integration_test", withResolver(resolver))
				return s, err
			},
			beforeSlavednss: []string{
				"root:root@tcp(192.168.1.1:13308)/integration_test",
			},
			afterSlavedns: []string{
				"root:root@tcp(192.168.1.1:13308)/integration_test",
				"root:root@tcp(192.168.1.2:13308)/integration_test",
				"root:root@tcp(192.168.1.3:13308)/integration_test",
			},
			cs: func() {
				time.Sleep(500 * time.Microsecond)
				resolver.mu.Lock()
				resolver.m["slaves.mycompany.com"] = []string{
					"192.168.1.1", "192.168.1.2", "192.168.1.3",
				}
				resolver.mu.Unlock()

			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := tc.slaves()
			require.NoError(t, err)
			assert.Equal(t, tc.beforeSlavednss, s.getSlaveDsns())
			go tc.cs()
			time.Sleep(2 * time.Second)
			assert.Equal(t, tc.afterSlavedns, s.getSlaveDsns())
		})
	}

}

func TestSlaves_Next(t *testing.T) {
	resolver := &mockResolver{
		m: map[string][]string{
			"slaves.mycompany.com": {
				"192.168.1.1",
				"192.168.1.2",
				"192.168.1.3",
			},
		},
		mu: &sync.RWMutex{},
	}
	testcases := []struct {
		name          string
		slaves        func() (*Slaves, error)
		next          func(s *Slaves) ([]string, error)
		wantSlaveName []string
		wantErr       error
	}{
		{
			name: "get single slave",
			slaves: func() (*Slaves, error) {
				s, err := NewSlaves("root:root@tcp(slaves.mycompany.com:13308)/integration_test",
					withResolver(resolver),
					WithInterval(time.Second),
					WithDSN(&mysql.Dsn{}),
					WithDriver("mysql"))
				return s, err
			},
			next: func(s *Slaves) ([]string, error) {
				ans := make([]string, 0, 1)
				slave, err := s.Next(context.Background())
				if err != nil {
					return ans, err
				}
				ans = append(ans, slave.SlaveName)
				return ans, nil
			},
			wantSlaveName: []string{"1"},
		},
		{
			name: "get mutil slave",
			slaves: func() (*Slaves, error) {
				s, err := NewSlaves("root:root@tcp(slaves.mycompany.com:13308)/integration_test", withResolver(resolver))
				return s, err
			},
			next: func(s *Slaves) ([]string, error) {
				ans := make([]string, 0, 4)
				for i := 0; i < 4; i++ {
					slave, err := s.Next(context.Background())
					if err != nil {
						return ans, err
					}
					ans = append(ans, slave.SlaveName)
				}
				return ans, nil
			},
			wantSlaveName: []string{"1", "2", "0", "1"},
		},
		{
			name: "Next timeout",
			slaves: func() (*Slaves, error) {
				s, err := NewSlaves("root:root@tcp(slaves.mycompany.com:13308)/integration_test", withResolver(resolver))
				return s, err
			},
			next: func(s *Slaves) ([]string, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				time.Sleep(2 * time.Second)
				slave, err := s.Next(ctx)
				cancel()
				if err != nil {
					return []string{}, err
				}
				return []string{slave.SlaveName}, nil
			},
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "do not have slave",
			slaves: func() (*Slaves, error) {
				r := &mockResolver{
					m: map[string][]string{
						"slaves.mycompany.com": {},
					},
					mu: &sync.RWMutex{},
				}
				s, err := NewSlaves("root:root@tcp(slaves.mycompany.com:13308)/integration_test", withResolver(r))
				return s, err
			},
			next: func(s *Slaves) ([]string, error) {
				ans := make([]string, 0, 1)
				slave, err := s.Next(context.Background())
				if err != nil {
					return ans, err
				}
				ans = append(ans, slave.SlaveName)
				return ans, nil
			},
			wantErr: errs.ErrSlaveNotFound,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := tc.slaves()
			require.NoError(t, err)
			names, err := tc.next(s)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantSlaveName, names)
		})
	}
}

func (s *Slaves) getSlaveDsns() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.slaveDsn
}
