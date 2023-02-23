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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMysqlParse_Parse(t *testing.T) {
	testcases := []struct {
		name    string
		dsn     string
		domain  string
		wantErr error
		m       *MysqlParse
		port    string
	}{
		{
			name:   "noraml dsn",
			m:      &MysqlParse{},
			dsn:    "root:root@tcp(slaves.mycompany.com:13308)/integration_test",
			domain: "slaves.mycompany.com",
			port:   "13308",
		},
		{
			name:   "noraml dsn",
			m:      &MysqlParse{},
			dsn:    "root:root@tcp(slaves.mycompany.com)/integration_test",
			domain: "slaves.mycompany.com",
			port:   "3306",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			domain, err := tc.m.Parse(tc.dsn)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.domain, domain)
		})
	}
}

func TestMysqlParse_Splice(t *testing.T) {
	testcases := []struct {
		name    string
		dsn     string
		wantDsn string
		ip      string
		wantErr error
	}{
		{
			name:    "normal case",
			dsn:     "root:root@tcp(slaves.mycompany.com:13308)/integration_test",
			wantDsn: "root:root@tcp(192.168.0.1:13308)/integration_test",
			ip:      "192.168.0.1",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			m := &MysqlParse{}
			_, _ = m.Parse(tc.dsn)
			ipdsn, err := m.Splice(tc.ip)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantDsn, ipdsn)
		})
	}
}
