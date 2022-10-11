package ruleengine

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-api-grapqhl/goparser"
	"go-api-grapqhl/graph/client"
	"math"
	"github.com/influxdata/influxdb1-client/models"
)

type TimeseriesMap struct {
	Time   []string             `json:"time" example:"[]"`
	Values []map[string]float64 `json:"values" example:"[]"`
}

func GenerateTimeseries(host string, port int, measurement, database, name, expression, starttime, endtime string) (*goparser.Function, []models.Row, error) {
	Fu := &goparser.Function{}
	err := Fu.GenerateFunctions(expression, name)
	if err != nil {
		return Fu, []models.Row{}, err
	}
	filter := "id"
	filterClause := "("
	for key := range Fu.Mapping {
		filterClause = filterClause + fmt.Sprintf(" \"%s\"='%s' OR ", filter, key)
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

func GenerateTimeseriesNew(host string, port int, measurement, database, name, 
		expression, starttime, endtime string, mapping map[string]string) (*goparser.Function, []models.Row, error) {
	Fu := &goparser.Function{}
	err := Fu.GenerateFunctions(expression, name)
	if err != nil {
		return Fu, []models.Row{}, err
	}
	filter := "id"
	filterClause := "("
	newMap := map[string]int{}
	for ke := range Fu.Mapping {
		newMap[mapping[ke]] = Fu.Mapping[ke]
	}
	fmt.Println(newMap)
	Fu.Mapping = newMap
	for key := range Fu.Mapping {
		filterClause = filterClause + fmt.Sprintf(" \"%s\"='%s' OR ", filter, key)
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

func GenerateNewTimeseries(f func(map[string]float64) float64, t TimeseriesMap, constants map[string]float64) [][]interface{} {
	values := [][]interface{}{}
	for ind, ele := range t.Time {
		input := t.Values[ind]
		for key := range constants {
			input[key] = constants[key]
		}
		// fmt.Println(input)
		val := f(input)
		if math.IsNaN(val) {
			values = append(values, []interface{}{ele, nil})
		} else {
			values = append(values, []interface{}{ele, val})
		}
		
	}
	return values
}

// func UpdateTimeseriesSchema(values [][]interface{}, name string, columns []string, tags map[string]string) NewRow {
// 	return NewRow{
// 		Name:    name,
// 		Tags:    tags,
// 		Columns: columns,
// 		Values:  values,
// 	}
// }
