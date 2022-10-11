package etlengine

import (
	// "encoding/json"
	// "errors"
	// "fmt"
	"go-api-grapqhl/goparser"
)

const (
	Exp = "exp"
	inq = "ineq"
)

type Timeseries struct {
	Time   []string             `json:"time" example:"[]"`
	Values []map[string]float64 `json:"values" example:"[]"`
}

func useConditionExpression(fMap []map[string]string) []goparser.ConditionExpression {
	ls := []goparser.ConditionExpression{}
	for _, mapping := range fMap {
		ls = append(ls, goparser.ConditionExpression{
			Expression: mapping[Exp],
			Inequality: mapping[inq],
		})
	}
	return ls
}

func toIfElse(fMap []map[string]string, uid int, name string) *goparser.IfElseCondition {
	return &goparser.IfElseCondition{
		Uid:        uid,
		Name:       name,
		Conditions: useConditionExpression(fMap),
	}
}

func GenerateIfElseCondition(fMap []map[string]string, uid int, name string) (func(map[string]float64) float64, error) {
	i := toIfElse(fMap, uid, name)
	f, _, err := i.ConditionFunction()
	return f, err
}

