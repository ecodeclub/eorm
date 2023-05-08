package eorm

import "database/sql"

type MultiExecRes struct {
	err error
	res []sql.Result
}

func (m MultiExecRes) Err() error {
	return m.err
}

func (m MultiExecRes) LastInsertId() (int64, error) {
	return m.res[len(m.res)-1].LastInsertId()
}
func (m MultiExecRes) RowsAffected() (int64, error) {
	var sum int64
	for _, r := range m.res {
		n, err := r.RowsAffected()
		if err != nil {
			return 0, err
		}
		sum += n
	}
	return sum, nil
}

func NewMultiExecRes(res []sql.Result) MultiExecRes {
	return MultiExecRes{res: res}
}
