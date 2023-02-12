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

package roundrobin

import (
	"context"
	"database/sql"
	"strconv"
	"sync/atomic"

	"github.com/gotomicro/eorm/internal/errs"
	"github.com/gotomicro/eorm/internal/slaves"
)

type Roundrobin struct {
	slaves []slaves.Slave
	cnt    uint32
}

func (r *Roundrobin) Next(ctx context.Context) (slaves.Slave, error) {
	if len(r.slaves) == 0 {
		return slaves.Slave{}, errs.ErrSlaveNotFound
	}
	cnt := atomic.AddUint32(&r.cnt, 1)
	index := int(cnt) % len(r.slaves)
	return r.slaves[index], nil
}

func NewRoundrobin(slavedbs ...*sql.DB) *Roundrobin {
	r := &Roundrobin{}
	r.slaves = make([]slaves.Slave, 0, len(slavedbs))
	for idx, slavedb := range slavedbs {
		s := slaves.Slave{
			SlaveName: strconv.Itoa(idx),
			DB:        slavedb,
		}
		r.slaves = append(r.slaves, s)
	}
	return r
}
