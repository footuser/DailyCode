package stack

import (
    "testing"
)

func TestStack(t *testing.T) {
	s := new(Stack)
	s.Push(5)
	if s.Pop() != 5 {
		t.Log("Pop err")
		t.Fail()
	}

}

