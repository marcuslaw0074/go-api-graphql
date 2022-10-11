package etlengine

import (
	// "encoding/json"
	// "errors"
	"encoding/json"
	"errors"
	"fmt"
	"go-api-grapqhl/goparser"
	"go-api-grapqhl/graph/client"
	"math"
	// "sort"
	"github.com/influxdata/influxdb1-client/models"
)

const (
	Exp = "exp"
	inq = "ineq"
)

type TimeseriesMap struct {
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

func GenerateTimeseriesNew(host string, port int, measurement, database, name,
	expression, starttime, endtime string, mapping map[string]string) (*goparser.Express, []models.Row, error) {
	Fu, err := goparser.InputExpression(expression)
	if err != nil {
		return Fu, []models.Row{}, err
	}
	filter := "id"
	filterClause := "("
	newMap := map[string]int{}
	for ke := range Fu.Mapping {
		newMap[mapping[ke]] = Fu.Mapping[ke]
	}
	Fu.Mapping = newMap
	for key := range Fu.Mapping {
		if len(key) > 0 {
			filterClause = filterClause + fmt.Sprintf(" \"%s\"='%s' OR ", filter, key)
		}
	}
	inter := "15m"
	filterClause = filterClause[:len(filterClause)-3] + ")"
	query := fmt.Sprintf(`SELECT MEAN(*) FROM %s WHERE (time>%s AND time<%s) AND %s group by time(%s), %s`,
		measurement, starttime, endtime, filterClause, inter, filter)
	fmt.Println(query)
	res, err := client.InfluxdbQuerySeriess(host, port, database, query)
	if err != nil {
		return Fu, []models.Row{}, err
	} else {
		return Fu, res, nil
	}
}

func GenerateNewTimeseries(f func(map[string]float64) bool, t TimeseriesMap, constants map[string]float64) ([][]interface{}, []string) {
	values := [][]interface{}{}
	example := t.Values[0]
	keys := []string{"time"}
	for key := range example {
		keys = append(keys, key)
	}
	// sort.Strings(keys)
	for ind, ele := range t.Time {
		input := t.Values[ind]
		for key := range constants {
			input[key] = constants[key]
		}
		val := f(input)
		if val {
			value := []interface{}{ele}
			for _, val := range keys {
				v, exists := input[val]
				if exists {
					value = append(value, v)
				}

			}
			values = append(values, value)
		}
	}
	return values, keys
}

func GenerateTimeseriesMap(r []models.Row) (TimeseriesMap, error) {
	lenn := len(r[0].Values)
	time := []string{}
	seriesMap := []map[string]float64{}
	i := 0
	for i < lenn {
		mapp := map[string]float64{}
		for ind, res := range r {
			id, exists := res.Tags["id"]
			if !exists {
				return TimeseriesMap{}, errors.New("cannot find id from tags")
			}
			row := res.Values[i]
			if ind == 0 {
				time = append(time, row[0].(string))
			}
			mapp[id] = math.NaN()
			if row[1] != nil {
				value, err := row[1].(json.Number).Float64()
				if err == nil {
					mapp[id] = value
				}
			}
		}
		seriesMap = append(seriesMap, mapp)
		i++
	}
	return TimeseriesMap{Time: time, Values: seriesMap}, nil
}
