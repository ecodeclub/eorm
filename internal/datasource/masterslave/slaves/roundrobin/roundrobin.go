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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	slaves2 "github.com/ecodeclub/eorm/internal/datasource/masterslave/slaves"

	"github.com/ecodeclub/eorm/internal/errs"
)

type slaves struct {
	slaves []slaves2.Slave
	cnt    uint32
}

func (r *slaves) Next(ctx context.Context) (slaves2.Slave, error) {
	if ctx.Err() != nil {
		return slaves2.Slave{}, ctx.Err()
	}
	if r == nil || len(r.slaves) == 0 {
		return slaves2.Slave{}, errs.ErrSlaveNotFound
	}
	cnt := atomic.AddUint32(&r.cnt, 1)
	index := int(cnt) % len(r.slaves)
	return r.slaves[index], nil
}

func (r *slaves) Close() error {
	var resErrs []string
	for _, inst := range r.slaves {
		err := inst.Close()
		if err != nil {
			resErrs = append(resErrs,
				fmt.Sprintf("slave DB name [%s] error: %s", inst.SlaveName, err.Error()))
		}
	}
	if resErrs != nil {
		return errors.New(strings.Join(resErrs, "; "))
	}
	return nil
}

func NewSlaves(dbs ...*sql.DB) (slaves2.Slaves, error) {
	r := &slaves{}
	r.slaves = make([]slaves2.Slave, 0, len(dbs))
	for idx, db := range dbs {
		s := slaves2.Slave{
			SlaveName: strconv.Itoa(idx),
			DB:        db,
		}
		r.slaves = append(r.slaves, s)
	}
	return r, nil
}
