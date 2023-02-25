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

package mysql

import (
	"strings"

	"github.com/ecodeclub/eorm/internal/errs"

	"github.com/go-sql-driver/mysql"
)

type Dsn struct {
	cfg    *mysql.Config
	domain string
	port   string
}

// Init 初始化
func (m *Dsn) Init(dsn string) error {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return err
	}
	index := strings.Index(cfg.Addr, ":")
	if index == -1 {
		return errs.NewInvalidDSNError(dsn)
	}
	m.domain = cfg.Addr[:index]
	m.port = cfg.Addr[index+1:]
	m.cfg = cfg
	return nil
}

func (m *Dsn) Domain() string {
	return m.domain
}

// FormatByIp 功能是利用目标 IP 拼接成一个 dsn
func (m *Dsn) FormatByIp(ip string) (string, error) {
	m.cfg.Addr = ip + ":" + m.port
	return m.cfg.FormatDSN(), nil
}
