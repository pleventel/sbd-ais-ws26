package main

import "fmt"

type Person struct {
	Name string
	Study string
	Age int
}

func (p Person) Print() {
	fmt.Print("Hello World!\n")
	fmt.Printf("My name is %s and I study %s. I'm %d years old.~n", p.Name, p.Study, p.Age)
}

func main() {
	person := Person{Name: "Levi", Study: "AIS", Age: 20}

	person.Print()
}