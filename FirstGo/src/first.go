package main 

import (
	"fmt"
)

func main() {
//	fmt.Printf("Hello World!")
	testFor()
}

func testFor() {
	for i := 0; i < 10; i++ {
		fmt.Printf("%d", i)
	}
}