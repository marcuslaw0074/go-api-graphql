package client

import (
	"encoding/json"
	"fmt"
	"go-api-grapqhl/tool"
	"log"
	"math"
	"sort"
	"sync"
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

type TimeseriesSchema struct {
	EquipmentName string
	FunctionType  string
	Id            string
	Series        []Timeseries
}

type TimeseriesSchemaNew struct {
	EquipmentName string
	FunctionType  string
	Id            string
	value         []float64
}

type TimeseriesSchemaNewNew struct {
	GroupBy map[string]string
	Series  [][]interface{}
}

type InfluxDBSchemaNew struct {
	TimeseriesAll   []TimeseriesSchemaNew
	TimeseriesGroup []TimeseriesGroupBy
	Time            []string
	mu              sync.Mutex
}

type InfluxDBSchemaNewNew struct {
	TimeseriesAll   []TimeseriesSchemaNewNew
	TimeseriesGroup []TimeseriesGroupByNew
	mu              sync.Mutex
}

type InfluxDBSchema struct {
	TimeseriesGroup []TimeseriesSchema
	mu              sync.Mutex
}

func GetTimeseriesFromSeriesNew(series models.Row) ([]string, []float64) {
	values := make([]float64, 0)
	time := make([]string, 0)
	for _, row := range series.Values {
		time = append(time, row[0].(string))
		if row[1] != nil {
			value, err := row[1].(json.Number).Float64()
			if err == nil {
				values = append(values, value)
				continue
			}
		}
		values = append(values, math.NaN())
	}
	return time, values
}

func GetTimeseriesFromSeries(series models.Row) []Timeseries {
	timeseries := make([]Timeseries, 0)
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
	return timeseries
}

func (i *InfluxDBSchema) UpdateTimeseriesGroup(t TimeseriesSchema) {
	i.mu.Lock()
	i.TimeseriesGroup = append(i.TimeseriesGroup, t)
	i.mu.Unlock()
}

func (i *InfluxDBSchemaNew) UpdateTimeseriesGroup(t TimeseriesSchemaNew) {
	i.mu.Lock()
	i.TimeseriesAll = append(i.TimeseriesAll, t)
	i.mu.Unlock()
}

func (i *InfluxDBSchemaNewNew) UpdateTimeseriesGroup(t TimeseriesSchemaNewNew) {
	i.mu.Lock()
	i.TimeseriesAll = append(i.TimeseriesAll, t)
	i.mu.Unlock()
}

func QueryTimeseries(query, database, host string, port int) *InfluxDBSchema {
	fmt.Println(query)
	influx := &InfluxDBSchema{}
	res, _ := InfluxdbQuery(query, database, host, port)
	wg := sync.WaitGroup{}
	wg.Add(len(res.Series))
	for _, series := range res.Series {
		go func(ser models.Row) {
			influx.UpdateTimeseriesGroup(TimeseriesSchema{
				EquipmentName: ser.Tags["EquipmentName"],
				FunctionType:  ser.Tags["FunctionType"],
				Id:            ser.Tags["id"],
				Series:        GetTimeseriesFromSeries(ser),
			})
			wg.Done()
		}(series)
	}
	wg.Wait()
	return influx
}

func (i *InfluxDBSchemaNew) findUniEquipment() []string {
	ls := []string{}
	for _, ele := range i.TimeseriesAll {
		if tool.StrContains(ls, ele.EquipmentName) < 0 {
			ls = append(ls, ele.EquipmentName)
		}
	}
	sort.Strings(ls)
	return ls
}

func (i *InfluxDBSchemaNew) findUniFunctionType() []string {
	ls := []string{}
	for _, ele := range i.TimeseriesAll {
		if tool.StrContains(ls, ele.FunctionType) < 0 {
			ls = append(ls, ele.FunctionType)
		}
	}
	sort.Strings(ls)
	return ls
}

func (i *InfluxDBSchemaNew) GroupByEquipmentName() {
	if len(i.TimeseriesAll) > 0 {
		funcTypes := i.findUniFunctionType()
		equip := i.findUniEquipment()
		for _, ele := range equip {
			e := TimeseriesGroupBy{
				GroupBy:        "EquipmentName",
				EquipmentNames: []string{ele},
				FunctionTypes:  funcTypes,
				values:         make([][]float64, 0),
			}
			for _, el := range funcTypes {
				for _, series := range i.TimeseriesAll {
					if series.EquipmentName == ele && series.FunctionType == el {
						e.values = append(e.values, series.value)
					}
				}
			}
			i.TimeseriesGroup = append(i.TimeseriesGroup, e)
		}
		i.TimeseriesAll = []TimeseriesSchemaNew{}
	} else if len(i.TimeseriesGroup) > 0 {

	}
}

func (i *InfluxDBSchemaNew) GroupByFunctionType() {
	if len(i.TimeseriesAll) > 0 {
		funcTypes := i.findUniFunctionType()
		equip := i.findUniEquipment()
		for _, ele := range funcTypes {
			e := TimeseriesGroupBy{
				GroupBy:        "FunctionType",
				EquipmentNames: equip,
				FunctionTypes:  []string{ele},
				values:         make([][]float64, 0),
			}
			for _, el := range equip {
				for _, series := range i.TimeseriesAll {
					if series.FunctionType == ele && series.EquipmentName == el {
						e.values = append(e.values, series.value)
					}
				}
			}
			i.TimeseriesGroup = append(i.TimeseriesGroup, e)
		}
		i.TimeseriesAll = []TimeseriesSchemaNew{}
	} else if len(i.TimeseriesGroup) > 0 {

	}
}

// func

// func (i *InfluxDBSchemaNew) ApplyFunctionGroupByEquipmentName(function func(...float64) float64, newFunctionType string) {
// 	if len(i.TimeseriesGroup) > 0 {
// 		for _, ele := range i.TimeseriesGroup {
// 			l := len(ele.values)
// 			for ind := range ele.values {
// 				for i :=
// 			}
// 		}
// 	}
// }

func (i *InfluxDBSchemaNewNew) findUniqueGroupByValues(key string, sortSlice bool) []string {
	ls := make([]string, 0)
	for _, ele := range i.TimeseriesAll {
		value, exists := ele.GroupBy[key]
		if !exists {
			return ls
		} else if tool.StrContains(ls, ele.GroupBy[key]) < 0 {
			ls = append(ls, value)
		}
	}
	if sortSlice {
		sort.Strings(ls)
	}
	return ls
}

func QueryTimeseriesNewNew(query, database, host string, port int) *InfluxDBSchemaNewNew {
	fmt.Println(query)
	influx := &InfluxDBSchemaNewNew{}
	res, _ := InfluxdbQuery(query, database, host, port)
	for _, series := range res.Series {
		influx.UpdateTimeseriesGroup(TimeseriesSchemaNewNew{
			GroupBy: series.Tags,
			Series:  series.Values,
		})
	}
	fmt.Println("finish query")
	return influx
}

func (i *InfluxDBSchemaNewNew) GroupTimeseries(key string, remove bool) {
	unique := i.findUniqueGroupByValues(key, true)
	l := make([]TimeseriesGroupByNew, 0)
	for _, unq := range unique {
		ls := TimeseriesGroupByNew{
			GroupByKey: map[string]string{key: unq},
		}
		for _, ele := range i.TimeseriesAll {
			val, exists := ele.GroupBy[key]
			if exists && val == unq {
				delete(ele.GroupBy, key)
				ls.GroupByValue = append(ls.GroupByValue, ele.GroupBy)
				ls.Series = append(ls.Series, ele.Series)
			}
		}
		l = append(l, ls)
	}
	i.TimeseriesGroup = l
	if remove {
		i.TimeseriesAll = []TimeseriesSchemaNewNew{}
	}
}

func MergeMap(old, new map[string]string) map[string]string {
	for key, val := range new {
		old[key] = val
	}
	return old
}

func (i *InfluxDBSchemaNewNew) UnGroupTimeseries(remove bool) {
	l := make([]TimeseriesSchemaNewNew, 0)
	for _, ele := range i.TimeseriesGroup {
		for in, el := range ele.GroupByValue {
			l = append(l, TimeseriesSchemaNewNew{
				GroupBy: MergeMap(ele.GroupByKey, el),
				Series:  ele.Series[in],
			})
		}
	}
	i.TimeseriesAll = l
	if remove {
		i.TimeseriesGroup = []TimeseriesGroupByNew{}
	}
}

func (i *InfluxDBSchemaNewNew) SortTimeseries(key string) {
	ct := 0
	for ct < len(i.TimeseriesGroup) {
		sort.Slice(i.TimeseriesGroup[ct].Series, func(k, j int) bool {
			return i.TimeseriesGroup[ct].GroupByValue[k][key] < i.TimeseriesGroup[ct].GroupByValue[j][key]
		})
		sort.Slice(i.TimeseriesGroup[ct].GroupByValue, func(k, j int) bool {
			return i.TimeseriesGroup[ct].GroupByValue[k][key] < i.TimeseriesGroup[ct].GroupByValue[j][key]
		})
		ct++
	}
}

type ApplySchema struct {
	Func          func(...float64) float64
	GroupByValue  map[string]string
	NewGroupByKey string
}

func (i *InfluxDBSchemaNewNew) ApplyFunction(f func(...float64) float64, GroupByValue map[string]string, toSeries bool) {
	for ind, ele := range i.TimeseriesGroup {
		l := make([][]interface{}, 0)
		for ind := range ele.Series[0] {
			ls := []float64{}
			time := ""
			for index := range ele.Series {
				val := ele.Series[index][ind]
				time = val[0].(string)
				if val[1] != nil {
					value, err := val[1].(json.Number).Float64()
					if err != nil {
						value = math.NaN()
					}
					ls = append(ls, value)
				} else {
					ls = append(ls, math.NaN())
				}
			}
			l = append(l, []interface{}{time, f(ls...)})
		}
		i.TimeseriesGroup[ind].Series = [][][]interface{}{l}
		i.TimeseriesGroup[ind].GroupByValue = []map[string]string{GroupByValue}
	}
}

func CopyMap(old map[string]string) map[string]string {
	l := map[string]string{}
	for key, val := range old {
		l[key] = val
	}
	return l
}

func AddGroupByValue(originGroupByKey, newGroupByValue map[string]string, newGroupByValueKey string) map[string]string {
	x := ""
	_, exists := newGroupByValue[newGroupByValueKey]
	if !exists {
		for _, val := range originGroupByKey {
			x = x + val + "_"
		}
		for _, val := range newGroupByValue {
			x = x + val + "_"
		}
		x = x[:len(x)-1]
		copiedMap := CopyMap(newGroupByValue)
		copiedMap[newGroupByValueKey] = x
		return copiedMap
	}
	return newGroupByValue
}

func (i *InfluxDBSchemaNewNew) ApplyFunctions(apply ...ApplySchema) {
	for ind, ele := range i.TimeseriesGroup {
		i.TimeseriesGroup[ind].Series = [][][]interface{}{}
		i.TimeseriesGroup[ind].GroupByValue = []map[string]string{}
		for _, app := range apply {
			l := make([][]interface{}, 0)
			for ind := range ele.Series[0] {
				ls := []float64{}
				time := ""
				for index := range ele.Series {
					val := ele.Series[index][ind]
					time = val[0].(string)
					if val[1] != nil {
						switch i := val[1].(type) {
						case json.Number:
							value, err := i.Float64()
							if err != nil {
								value = math.NaN()
							}
							ls = append(ls, value)
						case float64:
							ls = append(ls, float64(i))
						default:
							ls = append(ls, math.NaN())
						}
					} else {
						ls = append(ls, math.NaN())
					}
				}
				l = append(l, []interface{}{time, app.Func(ls...)})
			}
			newGroupByValue := AddGroupByValue(i.TimeseriesGroup[ind].GroupByKey, app.GroupByValue, app.NewGroupByKey)
			i.TimeseriesGroup[ind].Series = append(i.TimeseriesGroup[ind].Series, l)
			i.TimeseriesGroup[ind].GroupByValue = append(i.TimeseriesGroup[ind].GroupByValue, newGroupByValue)
		}
	}
}

func QueryTimeseriesNew(query, database, host string, port int) *InfluxDBSchemaNew {
	fmt.Println(query)
	timels := make([]string, 0)
	influx := &InfluxDBSchemaNew{}
	res, _ := InfluxdbQuery(query, database, host, port)
	wg := sync.WaitGroup{}
	wg.Add(len(res.Series))
	for _, series := range res.Series {
		go func(ser models.Row) {
			time, values := GetTimeseriesFromSeriesNew(ser)
			influx.UpdateTimeseriesGroup(TimeseriesSchemaNew{
				EquipmentName: ser.Tags["EquipmentName"],
				FunctionType:  ser.Tags["FunctionType"],
				Id:            ser.Tags["id"],
				value:         values,
			})
			timels = time
			wg.Done()
		}(series)
	}
	wg.Wait()
	influx.Time = timels
	return influx
}

type TimeseriesGroupBy struct {
	GroupBy        string
	values         [][]float64
	EquipmentNames []string
	FunctionTypes  []string
}

type TimeseriesGroupByNew struct {
	Series       [][][]interface{}
	GroupByKey   map[string]string
	GroupByValue []map[string]string
}

func findUniEquipment(t []TimeseriesSchema) []string {
	ls := []string{}
	for _, ele := range t {
		if tool.StrContains(ls, ele.EquipmentName) > -1 {
			ls = append(ls, ele.EquipmentName)
		}
	}
	return ls
}

func (i *InfluxDBSchema) findTimeseriesByEquipFunc(equip, funct string) []Timeseries {
	for _, ele := range i.TimeseriesGroup {
		if ele.EquipmentName == equip && ele.FunctionType == funct {
			return ele.Series
		}
	}
	return []Timeseries{}
}

func (i *InfluxDBSchema) QueryTimeseriesGroup(query, database, host string, port int) {
	fmt.Println(query)
	res, _ := InfluxdbQuery(query, database, host, port)
	wg := sync.WaitGroup{}
	wg.Add(len(res.Series))
	for _, series := range res.Series {
		go func(ser models.Row) {
			i.UpdateTimeseriesGroup(TimeseriesSchema{
				EquipmentName: ser.Tags["EquipmentName"],
				FunctionType:  ser.Tags["FunctionType"],
				Id:            ser.Tags["id"],
				Series:        GetTimeseriesFromSeries(ser),
			})
			wg.Done()
		}(series)
	}
	wg.Wait()
}

func QueryDfGroup(query, database, host string, port int) []tool.GroupDataframe {
	fmt.Println(query)
	res, _ := InfluxdbQuery(query, database, host, port)
	dfGroup := make([]tool.GroupDataframe, 0)
	equipmentList := make([]string, 0)
	for ind, series := range res.Series {
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
		fmt.Println(ind, "to df")
		dfNew := dataframe.LoadStructs(timeseries)
		if tool.StringInSlice(equipmentName, equipmentList) {
			ind, err := tool.FindEleByEquip(dfGroup, equipmentName)
			if err == nil {
				df := dfGroup[ind].Dataframe
				name := df.Names()
				strs := []string{name[len(name)-1], functionType}
				sort.Strings(strs)
				fmt.Println(ind, "to inner")
				if strs[len(strs)-1] == functionType {
					dfGroup[ind].Dataframe = df.InnerJoin(dfNew.Rename(functionType, "Value"), "Time")
				} else {
					dfGroup[ind].Dataframe = dfNew.Rename(functionType, "Value").InnerJoin(df, "Time")
				}
				fmt.Println(ind, "end inner")
			}
		} else {
			dfGroup = append(dfGroup, tool.GroupDataframe{
				EquipmentName: equipmentName,
				Dataframe:     dfNew.Rename(functionType, "Value"),
			})
		}
		equipmentList = append(equipmentList, equipmentName)
	}
	fmt.Println("finish query")
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
				Block:         newGroupBy[0],
				BuildingName:  newGroupBy[1],
				EquipmentName: newGroupBy[2],
				FunctionType:  newGroupBy[3],
				Id:            newGroupBy[4],
				Prefername:    newGroupBy[5],
				Dataframe:     dfNew.Rename(functionType, "Value"),
			})
		}
		equipmentList = append(equipmentList, equipmentName)
	}
	return dfGroup
}

func ApplyFunctionDfGroup(dfGroup []tool.GroupDataframe, function func(...float64) float64, newFunctionType string, indCol ...int) []tool.GroupDataframe {
	for ind, ele := range dfGroup {
		dfGroup[ind] = tool.GroupDataframe{
			EquipmentName: ele.EquipmentName,
			Dataframe:     ele.Dataframe.Rapply(tool.ApplyFunction(function, indCol...)).Rename(fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), "X0").Mutate(ele.Dataframe.Col("Time")),
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
