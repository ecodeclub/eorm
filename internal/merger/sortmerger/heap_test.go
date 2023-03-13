package sortmerger

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHp(nodes []*node, columns sortColumns) *Hp {
	h := &Hp{
		sortColumns: columns,
	}
	for _, node := range nodes {
		heap.Push(h, node)
	}
	return h
}

func newTestNodes(sortColsList [][]any) []*node {
	res := make([]*node, 0, len(sortColsList))
	for _, sortCols := range sortColsList {
		n := &node{
			sortCols: sortCols,
		}
		res = append(res, n)
	}
	return res
}

func TestHeap(t *testing.T) {
	testcases := []struct {
		name      string
		nodes     func() []*node
		wantNodes func() []*node
		sortCols  func() sortColumns
	}{
		{
			name: "单个列升序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2},
					{5},
					{6},
					{1},
					{0},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{0},
					{1},
					{2},
					{5},
					{6},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn[int]("id", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "单个列降序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2},
					{5},
					{6},
					{1},
					{0},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{6},
					{5},
					{2},
					{1},
					{0},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn[int]("id", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列顺序：升序,降序,升序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{1, "e", 1},
					{1, "e", 2},
					{1, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{2, "e", 1},
					{2, "e", 2},
					{2, "e", 3},
					{2, "b", 1},
					{2, "a", 1},
					{5, "e", 1},
					{5, "e", 2},
					{5, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn[int]("id", ASC), NewSortColumn[string]("name", DESC), NewSortColumn[int]("age", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列顺序：降序,升序,降序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{5, "a", 1},
					{5, "b", 1},
					{5, "e", 3},
					{5, "e", 2},
					{5, "e", 1},
					{2, "a", 1},
					{2, "b", 1},
					{2, "e", 3},
					{2, "e", 2},
					{2, "e", 1},
					{1, "a", 1},
					{1, "b", 1},
					{1, "e", 3},
					{1, "e", 2},
					{1, "e", 1},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn[int]("id", DESC), NewSortColumn[string]("name", ASC), NewSortColumn[int]("age", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列的顺序: 升序,升序,降序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{1, "a", 1},
					{1, "b", 1},
					{1, "e", 3},
					{1, "e", 2},
					{1, "e", 1},
					{2, "a", 1},
					{2, "b", 1},
					{2, "e", 3},
					{2, "e", 2},
					{2, "e", 1},
					{5, "a", 1},
					{5, "b", 1},
					{5, "e", 3},
					{5, "e", 2},
					{5, "e", 1},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn[int]("id", ASC), NewSortColumn[string]("name", ASC), NewSortColumn[int]("age", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列的顺序: 降序,降序,升序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{5, "e", 1},
					{5, "e", 2},
					{5, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{2, "e", 1},
					{2, "e", 2},
					{2, "e", 3},
					{2, "b", 1},
					{2, "a", 1},
					{1, "e", 1},
					{1, "e", 2},
					{1, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn[int]("id", DESC), NewSortColumn[string]("name", DESC), NewSortColumn[int]("age", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列的顺序: 降序,降序,降序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{5, "e", 3},
					{5, "e", 2},
					{5, "e", 1},
					{5, "b", 1},
					{5, "a", 1},
					{2, "e", 3},
					{2, "e", 2},
					{2, "e", 1},
					{2, "b", 1},
					{2, "a", 1},
					{1, "e", 3},
					{1, "e", 2},
					{1, "e", 1},
					{1, "b", 1},
					{1, "a", 1},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn[int]("id", DESC), NewSortColumn[string]("name", DESC), NewSortColumn[int]("age", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列的顺序: 升序,升序,升序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{1, "a", 1},
					{1, "b", 1},
					{1, "e", 1},
					{1, "e", 2},
					{1, "e", 3},
					{2, "a", 1},
					{2, "b", 1},
					{2, "e", 1},
					{2, "e", 2},
					{2, "e", 3},
					{5, "a", 1},
					{5, "b", 1},
					{5, "e", 1},
					{5, "e", 2},
					{5, "e", 3},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn[int]("id", ASC), NewSortColumn[string]("name", ASC), NewSortColumn[int]("age", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHp(tc.nodes(), tc.sortCols())
			res := make([]*node, 0, h.Len())
			for h.Len() > 0 {
				res = append(res, heap.Pop(h).(*node))
			}
			assert.Equal(t, tc.wantNodes(), res)
		})
	}

}
