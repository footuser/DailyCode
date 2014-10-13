package stack

import (

)

type Stack struct {
	i int
	data [10]int
}

func (s *Stack) Push (k int) {
	s.data[s.i] = k
	s.i ++
}

func (s *Stack) Pop() (res int) {
	s.i --
	res = s.data[s.i]
	return 
}