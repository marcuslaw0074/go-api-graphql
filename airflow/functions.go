package airflow

import (
	"fmt"
	"time"
)

type BaseFunction struct{}

func (f BaseFunction) Test() (string, error) {
	fmt.Println("hk")
	time.Sleep(time.Second * 3)
	return "ok", nil
}

func (f BaseFunction) Test2() (string, error) {
	fmt.Println("hk2")
	time.Sleep(time.Second * 6)
	return "ok", nil
}

func (f BaseFunction) Test3() (string, error) {
	fmt.Println("hk3")
	time.Sleep(time.Second * 1)
	return "ok", nil
}
