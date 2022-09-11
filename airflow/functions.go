package airflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-api-grapqhl/graph/client"
	"math"
	"time"
	"github.com/go-gota/gota/dataframe"
)

type BaseFunction struct{}

type Timeseries struct {
	Time string `json:"time"`
	Value float64 `json:"value" format:"float64"`
}

type AllDataframe struct {
	EquipmentName string
	FunctionType string
	Id string
	Dataframe dataframe.DataFrame
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
	Dataframe dataframe.DataFrame
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
	fmt.Println("hk")
	res, _ := client.InfluxdbQuery(`SELECT MEAN(value) FROM Utility_3 
					WHERE "FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
					 "FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' AND 
					 time>now()-40m GROUP BY EquipmentName, FunctionType, id, time(20m) `, 
					 "WIIOT")
	// store := make([]AllDataframe, 0)
	dfGroup := make([]GroupDataframe, 0)
	fmt.Println(res)
	equipmentList := make([]string, 0)
	for _, series := range res.Series {
		fmt.Println(series)
		timeseries := make([]Timeseries, 0)
		var id string = series.Tags["id"]
		fmt.Println(id)
		var equipmentName string = series.Tags["EquipmentName"]
		var functionType string = series.Tags["FunctionType"]
		for _, row := range series.Values {
			if row[1] != nil {
				value, err := row[1].(json.Number).Float64()
				if err == nil {
					timeseries = append(timeseries, Timeseries{
						Time: row[0].(string),
						Value: value,
					})
				} else {
					timeseries = append(timeseries, Timeseries{
						Time: row[0].(string),
						Value: math.NaN(),
					})
				}
			} else {
				timeseries = append(timeseries, Timeseries{
					Time: row[0].(string),
					Value: math.NaN(),
				})
			}
		}
		dfNew := dataframe.LoadStructs(timeseries)
		if stringInSlice(equipmentName, equipmentList) {
			ind, err := findEleByEquip(dfGroup, equipmentName)
			if err == nil {
				df := dfGroup[ind].Dataframe
				df = df.InnerJoin(dfNew.Rename(functionType, "Value"), "Time")
				dfGroup[ind].Dataframe = df
			}
		} else {
			dfGroup = append(dfGroup, GroupDataframe{
				EquipmentName: equipmentName,
				Dataframe: dfNew.Rename(functionType, "Value"),
			})
		}
		equipmentList = append(equipmentList, equipmentName)
		// store = append(store, AllDataframe{
		// 	EquipmentName: equipmentName,
		// 	FunctionType: functionType,
		// 	Id: id,
		// 	Dataframe: dfNew,
		// })
	}
	// for _, equip := range equipmentList {
	// 	for _, ele := range store {

	// 	}
	// }
	fmt.Println(dfGroup)
	// fmt.Println(store)
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
