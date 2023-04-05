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

package roundrobin

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync/atomic"

	"go.uber.org/multierr"

	"github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"

	"github.com/ecodeclub/eorm/internal/errs"
)

type Slaves struct {
	slaves []slaves.Slave
	cnt    uint32
}

func (r *Slaves) Next(ctx context.Context) (slaves.Slave, error) {
	if ctx.Err() != nil {
		return slaves.Slave{}, ctx.Err()
	}
	if r == nil || len(r.slaves) == 0 {
		return slaves.Slave{}, errs.ErrSlaveNotFound
	}
	cnt := atomic.AddUint32(&r.cnt, 1)
	index := int(cnt) % len(r.slaves)
	return r.slaves[index], nil
}

func (r *Slaves) Close() error {
	var err error
	for _, inst := range r.slaves {
		if er := inst.Close(); er != nil {
			err = multierr.Combine(
				err, fmt.Errorf("slave DB name [%s] error: %w", inst.SlaveName, er))
		}
	}
	return err
}

func NewSlaves(dbs ...*sql.DB) (*Slaves, error) {
	r := &Slaves{}
	r.slaves = make([]slaves.Slave, 0, len(dbs))
	for idx, db := range dbs {
		s := slaves.Slave{
			SlaveName: strconv.Itoa(idx),
			DB:        db,
		}
		r.slaves = append(r.slaves, s)
	}
	return r, nil
}
