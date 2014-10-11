package main 

import (
	"fmt"
)

type stack struct {
	i int
	data [2]int
}

func (s *stack) push (k int) {
	s.data[s.i] = k;
	s.i ++;
}

func (s *stack) pop () int {
	s.i --;
	return s.data[s.i]
}

func main() {
	s := new (stack)
	s.push(1)
	s.push(22)
	fmt.Printf("%v\n", s)
	fmt.Printf("%d\n", s.pop)
}

func push(i int) bool{
	
	return false
}