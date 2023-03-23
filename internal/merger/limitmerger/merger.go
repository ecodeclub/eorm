package limitmerger

import (
	"context"
	"database/sql"
	"sync"

	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
)

type Merger struct {
	m      merger.Merger
	limit  int
	offset int
}

func NewMerger(m merger.Merger, offset int, limit int) *Merger {
	return &Merger{
		m:      m,
		limit:  limit,
		offset: offset,
	}
}

func (m *Merger) Merge(ctx context.Context, results []*sql.Rows) (merger.Rows, error) {
	rows, err := m.m.Merge(ctx, results)
	if err != nil {
		return nil, err
	}
	err = m.nextOffset(ctx, rows)
	if err != nil {
		return nil, err
	}
	return &Rows{
		rows:  rows,
		mu:    &sync.RWMutex{},
		limit: m.limit,
	}, nil
}

func (m *Merger) nextOffset(ctx context.Context, rows merger.Rows) error {
	offset := m.offset
	for i := 0; i < offset; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// 如果偏移量超过rows结果集返回的行数，不会报错。用户最终查到0行
		if !rows.Next() {
			if rows.Err() != nil {
				return rows.Err()
			}
			break
		}
	}
	return nil
}

type Rows struct {
	rows    merger.Rows
	limit   int
	cnt     int
	lastErr error
	closed  bool
	mu      *sync.RWMutex
}

func (r *Rows) Next() bool {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false
	}
	if r.cnt >= r.limit || r.lastErr != nil {
		r.mu.Unlock()
		_ = r.Close()
		return false
	}
	canNext, err := r.next()
	if err != nil {
		r.lastErr = err
		r.mu.Unlock()
		_ = r.Close()
		return false
	}
	if !canNext {
		r.mu.Unlock()
		_ = r.Close()
		return canNext
	}
	r.cnt++
	r.mu.Unlock()
	return canNext
}
func (r *Rows) next() (bool, error) {
	if r.rows.Next() {
		return true, nil
	}
	if r.rows.Err() != nil {
		return false, r.rows.Err()
	}
	return false, nil
}

func (r *Rows) Scan(dest ...any) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.lastErr != nil {
		return r.lastErr
	}
	if r.closed {
		return errs.ErrMergerRowsClosed
	}
	return r.rows.Scan(dest...)
}

func (r *Rows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return r.rows.Close()
}

func (r *Rows) Columns() ([]string, error) {
	return r.rows.Columns()
}

func (r *Rows) Err() error {
	return r.lastErr
}
