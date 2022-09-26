package client

import (
	"encoding/json"
	"fmt"
	"go-api-grapqhl/tool"
	"log"
	"math"
	"sort"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/influxdata/influxdb1-client/models"
	influx "github.com/influxdata/influxdb1-client/v2"
)

type InfluxWriteSchema struct {
	Name   string
	Tags   map[string]string
	Fields map[string]interface{}
	T      time.Time
}

type Timeseries struct {
	Time  string  `json:"time"`
	Value float64 `json:"value" format:"float64"`
}

func InfluxdbQuery(query, database, host string, port int) (influx.Result, error) {
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr: fmt.Sprintf("http://%s:%v", host, port),
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
		return influx.Result{}, err
	}
	defer c.Close()
	q := influx.NewQuery(query, database, "")
	if response, err := c.Query(q); err == nil && response.Error() == nil {
		return response.Results[0], nil
	}
	return influx.Result{}, err
}

func InfluxdbQuerySeries(host string, port int, database, query string) (models.Row, error) {
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr: fmt.Sprintf("http://%s:%v", host, port),
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
		return models.Row{}, err
	}
	defer c.Close()
	q := influx.NewQuery(query, database, "")
	if response, err := c.Query(q); err == nil && response.Error() == nil {
		return response.Results[0].Series[0], nil
	}
	return influx.Result{}.Series[0], err
}

func InfluxdbWritePoints(url, database string, points []InfluxWriteSchema) error {
	lenn := len(points)
	log.Printf("Start Writing %v into influxDB \n", lenn)
	if lenn == 0 {
		return nil
	}
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		// Addr: "http://192.168.100.216:18086",
		Addr: url,
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
		return err
	}
	defer c.Close()
	bp, _ := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:  database,
		Precision: "s",
	})
	for _, ele := range points {
		pt, err := influx.NewPoint(ele.Name, ele.Tags, ele.Fields, ele.T)
		if err != nil {
			return err
		} else {
			bp.AddPoint(pt)
		}
	}
	return c.Write(bp)
}

func QueryDfGroup(query, database, host string, port int) []tool.GroupDataframe {
	fmt.Println(query)
	res, _ := InfluxdbQuery(query, database, host, port)
	fmt.Println(res)
	dfGroup := make([]tool.GroupDataframe, 0)
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
		if tool.StringInSlice(equipmentName, equipmentList) {
			ind, err := tool.FindEleByEquip(dfGroup, equipmentName)
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
			dfGroup = append(dfGroup, tool.GroupDataframe{
				EquipmentName: equipmentName,
				Dataframe:     dfNew.Rename(functionType, "Value"),
			})
		}
		equipmentList = append(equipmentList, equipmentName)
	}
	return dfGroup
}

func QueryDfGroupBy(query, database, host string, port int, groupBy ...string) []tool.AllGroupDataframe {
	groupBy = tool.FindGroupByList(groupBy...)
	res, _ := InfluxdbQuery(query, database, host, port)
	dfGroup := make([]tool.AllGroupDataframe, 0)
	equipmentList := make([]string, 0)
	for _, series := range res.Series {
		timeseries := make([]Timeseries, 0)
		newGroupBy := make([]string, 0)
		for _, ele := range groupBy {
			st, err := series.Tags[ele]
			if err {
				newGroupBy = append(newGroupBy, st)
			} else {
				newGroupBy = append(newGroupBy, "")
			}
		}
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
		if tool.StringInSlice(equipmentName, equipmentList) {
			ind, err := tool.FindEleByEquipAll(dfGroup, equipmentName)
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
			dfGroup = append(dfGroup, tool.AllGroupDataframe{
				Block: newGroupBy[0],
				BuildingName: newGroupBy[1],
				EquipmentName: newGroupBy[2],
				FunctionType: newGroupBy[3],
				Id: newGroupBy[4],
				Prefername: newGroupBy[5],
				Dataframe:     dfNew.Rename(functionType, "Value"),
			})
		}
		equipmentList = append(equipmentList, equipmentName)
	}
	return dfGroup
}

func ApplyFunctionDfGroup(dfGroup []tool.GroupDataframe, function func (...float64) float64, newFunctionType string, indCol ...int) []tool.GroupDataframe {
	for ind, ele := range dfGroup {
		dfGroup[ind] = tool.GroupDataframe{
			EquipmentName: ele.EquipmentName,
			Dataframe: ele.Dataframe.Rapply(tool.ApplyFunction(function, indCol...)).Rename(fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), "X0").Mutate(ele.Dataframe.Col("Time")),
		}
	}
	return dfGroup
}

func WriteDfGroup(query, database, measurement, EquipmentName, FunctionType, id string, df dataframe.DataFrame, startIndex int) []InfluxWriteSchema {
	lsss := make([]InfluxWriteSchema, 0)
	serValue := df.Col("Value")
	serTime := df.Col("Time").Records()
	for ind2, ele2 := range serValue.Float() {
		if ind2 > startIndex-1 {
			t, err := time.Parse("2006-01-02T15:04:05Z", serTime[ind2])
			if err == nil {
				if !math.IsNaN(ele2) {
					var newId string
					if len(id) > 0 {
						newId = id
					} else {
						newId = fmt.Sprintf("%s_%s", EquipmentName, FunctionType)
					}
					lsss = append(lsss, InfluxWriteSchema{
						Name: measurement,
						Tags: map[string]string{
							"EquipmentName": EquipmentName,
							"FunctionType":  FunctionType,
							"id":            newId,
							"prefername":    newId,
							"BuildingName":  database,
							"Block":         measurement,
						},
						Fields: map[string]interface{}{"value": ele2},
						T:      t,
					})
				}
			}
		}

	}
	return lsss
}


func UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id string, df dataframe.DataFrame, startIndex int) error {
	lsss := WriteDfGroup(query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
	err := InfluxdbWritePoints(url, database, lsss)
	return err
}