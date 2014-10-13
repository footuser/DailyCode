package main 

import (
	"fmt"
	. "stack"
)

func main() {
//	testGoto()
//	testArr()
//	fizzBuzz()
//	a3_1()
//	reverse() 
//	arr := [...]float64{1,2,3,4,5,6}
//	print(a4_1(arr[:]))
//	i,j := a6(8, 8)
//	print(i,j)
	s := new (Stack)
	s.Push(1)
//	s.Push(22)
//	fmt.Printf("%v\n", s)
//	fmt.Printf("%d\n", s.Pop)

	var c chan int
	c = make(chan int)
	quit := make(chan bool)
	go shower(c, quit)
	for i:=1; i<10; i++ {
		if i > 5 {
			quit <- true
		} else {
			c <- i
		}
	}

}

func shower(c chan int, quit chan bool) {
	for {
		select {
			case i := <- c:
				println(i)
			case <- quit:
				break
		}
		
	}
}

func testGoto() {
	i := 0
Loop:
	fmt.Printf("%d\n", i)
	if i < 10 {
		i++;
		goto Loop
	}
}

func testArr() {
	arr := [...]int{1,2,3,4,5,6}
	fmt.Printf("%v\n", arr)
}

func fizzBuzz() {
	for i := 1; i <= 100; i++ {
		switch {
			case i%3 == 0 && i%5 != 0:
				println("Fizz")
			case i%5 == 0 && i%3 != 0:
				println("Buzz")
			case i%3 == 0 && i%5 == 0:
				println("FizzBuzz")
			default:
				println(i)
		}
	}
}

func a3_1() {
	for i := 1; i < 100; i++ {
		for j := 0; j < i; j++ {
			print("A")
		}
		println()
	}
}

func reverse() {
	str := "football"
	tmp := []rune(str)
	for i, j := 0, len(tmp) - 1; i < j; i, j = i + 1, j - 1 {
		tmp[i], tmp[j] = tmp[j], tmp[i]
	}
	println(str)
	println(string(tmp))
}

func a4_1(t []float64) (result float64) {
	sum := 0.0
	switch len(t) {
		case 0:
			return 
		default:
			for _,v := range t {
				sum += v
			}
			result = sum / float64(len(t))
	}
	return 
}

func a6(i,j int) (x,y int) {
	switch {
		case i - j < 0, i - j == 0:
			x,y = i,j
		case i - j > 0:
			x,y = j,i
	}
	return
}