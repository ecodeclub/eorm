package aggregator

import (
	"testing"

	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	"github.com/stretchr/testify/assert"
)

func TestSum_Aggregate(t *testing.T) {
	testcases := []struct {
		name    string
		input   [][]any
		wantVal any
		wantErr error
	}{
		{
			name: "sum正常合并",
			input: [][]any{
				{
					int64(10),
				},
				{
					int64(20),
				},
				{
					int64(30),
				},
			},
			wantVal: int64(60),
		},
		{
			name: "传入空切片",
			input: [][]any{
				{},
			},
			wantErr: errs.ErrMergerAggregateParticipant,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			sum := NewSUM(NewColInfo(0, "SUM(id)"), "SUM(id)")
			val, err := sum.Aggregate(tc.input)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, val)
		})
	}

}
