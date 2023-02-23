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
	"strings"

	"github.com/go-sql-driver/mysql"
)

type ParseDSN interface {
	Parse(dsn string) (domain string, err error)
	Splice(ip string) (dsn string, err error)
}

type MysqlParse struct {
	cfg  *mysql.Config
	port string
}

// Parse 功能是从dsn中解析出domain
func (m *MysqlParse) Parse(dsn string) (domain string, err error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	index := strings.Index(cfg.Addr, ":")
	domain = cfg.Addr[:index]
	m.port = cfg.Addr[index+1:]
	m.cfg = cfg
	return domain, nil

}

// Splice 功能是将Ip拼成dsn
func (m *MysqlParse) Splice(ip string) (dsn string, err error) {
	m.cfg.Addr = ip + ":" + m.port
	return m.cfg.FormatDSN(), nil
}
