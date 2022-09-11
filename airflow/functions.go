package airflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-api-grapqhl/graph/client"
	"math"
	"sort"
	// "strings"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"time"
)

type BaseFunction struct{}

type Timeseries struct {
	Time  string  `json:"time"`
	Value float64 `json:"value" format:"float64"`
}

type AllDataframe struct {
	EquipmentName string
	FunctionType  string
	Id            string
	Dataframe     dataframe.DataFrame
}

func IntContains(list []int, str int) (int, bool) {
	for index, a := range list {
		if a == str {
			return index, true
		}
	}
	return -1, false
}

func sliceFilledWithString(size int, str string) []string {
	data := make([]string, size)
	for i := 0; i < size; i++ {
		data[i] = str
	}
	return data
}

func ApplyFunction(function func(...float64) float64, indCol ...int) func(series.Series) series.Series {
	return func(s series.Series) series.Series {
		floats := s.Float()
		list := make([]float64, 0)
		for index, value := range floats {
			_, contains := IntContains(indCol, index)
			if contains {
				list = append(list, value)
			}
		}
		return series.Floats(function(list...))
	}
}

func findElementByEquip(s []AllDataframe, equipment string) (int, error) {
	for ind, ele := range s {
		if ele.EquipmentName == equipment {
			return ind, nil
		}
	}
	return -1, errors.New("cannot find dataframe")
}

func findEleByEquip(s []GroupDataframe, equipment string) (int, error) {
	for ind, ele := range s {
		if ele.EquipmentName == equipment {
			return ind, nil
		}
	}
	return -1, errors.New("cannot find dataframe")
}

type GroupDataframe struct {
	EquipmentName string
	Dataframe     dataframe.DataFrame
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func (f BaseFunction) Test() (string, error) {
	newFunctionType := "Chiller_Delta_T"
	measurement := "Utility_3"
	database := "WIIOT"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, measurement)
	res, _ := client.InfluxdbQuery(query, database)
	fmt.Println(query)
	dfGroup := make([]GroupDataframe, 0)
	equipmentList := make([]string, 0)
	for _, series := range res.Series {
		timeseries := make([]Timeseries, 0)
		var equipmentName string = series.Tags["EquipmentName"]
		var functionType string = series.Tags["FunctionType"]
		for _, row := range series.Values {
			if row[1] != nil {
				value, err := row[1].(json.Number).Float64()
				if err == nil {
					timeseries = append(timeseries, Timeseries{
						Time:  row[0].(string),
						Value: value,
					})
				} else {
					timeseries = append(timeseries, Timeseries{
						Time:  row[0].(string),
						Value: math.NaN(),
					})
				}
			} else {
				timeseries = append(timeseries, Timeseries{
					Time:  row[0].(string),
					Value: math.NaN(),
				})
			}
		}
		dfNew := dataframe.LoadStructs(timeseries)
		if stringInSlice(equipmentName, equipmentList) {
			ind, err := findEleByEquip(dfGroup, equipmentName)
			if err == nil {
				df := dfGroup[ind].Dataframe
				name := df.Names()
				strs := []string{name[len(name)-1], functionType}
				sort.Strings(strs)
				if strs[len(strs)-1] == functionType {
					dfGroup[ind].Dataframe = df.InnerJoin(dfNew.Rename(functionType, "Value"), "Time")
				} else {
					dfGroup[ind].Dataframe = dfNew.Rename(functionType, "Value").InnerJoin(df, "Time")
				}
			}
		} else {
			dfGroup = append(dfGroup, GroupDataframe{
				EquipmentName: equipmentName,
				Dataframe:     dfNew.Rename(functionType, "Value"),
			})
		}
		equipmentList = append(equipmentList, equipmentName)
	}
	lss := []int{1, 2}
	for ind, ele := range dfGroup {
		dfGroup[ind].Dataframe = ele.Dataframe.Rapply(ApplyFunction(func(f ...float64) float64 {
			return f[0] - f[1]
		}, lss...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		lsss := make([]client.InfluxWriteSchema, 0)
		serValue := dfGroup[ind].Dataframe.Col("Value")
		serTime := dfGroup[ind].Dataframe.Col("Time").Records()
		for ind2, ele2 := range serValue.Float() {
			t, err := time.Parse("2006-01-02T15:04:05Z", serTime[ind2])
			if err != nil {
				t = time.Now()
			}
			if !math.IsNaN(ele2) {
				lsss = append(lsss, client.InfluxWriteSchema{
					Name: measurement,
					Tags: map[string]string{
						"EquipmentName": ele.EquipmentName,
						"FunctionType":  newFunctionType,
						"id":            fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType),
						"prefername":    fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType),
						"BuildingName":  database,
						"Block":         measurement,
					},
					Fields: map[string]interface{}{"value": ele2},
					T:      t,
				})
			}
		}
		fmt.Println(lsss)
		t := client.InfluxdbWritePoints(lsss, "WIIOT")
		fmt.Println(t)
	}
	dfDict := dfGroup[0].Dataframe.Maps()
	fmt.Println(dfDict)
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
