package main

import (
	"fmt"
	//"strconv"
	"time"
)

func main() {
	fmt.Println(0)
	response := make(chan int)
	go func() {
		x := 1
		time.Sleep(2 * time.Second)
		fmt.Println("Inside")
		response <- x
	}()
	fmt.Println(2)
	fmt.Println(3)
	fmt.Println(4)
	fmt.Println(<-response)
	time.Sleep(1 * time.Second)
	fmt.Println(5)
	//output := <- response
}
