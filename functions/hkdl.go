package functions

import (
	"fmt"
	"go-api-grapqhl/graph/client"
	logging "go-api-grapqhl/log"
	"go-api-grapqhl/tool"
	"math"
	"sync"
	"time"

	"github.com/go-gota/gota/dataframe"
)

var HKDL_Chiller = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
var HKDL_CT = []int{}
var HKDL_Logger = logging.StartLogger("log/HKDL_LogFile.log")

var TimeClause_HKDL string = "time>='2018-04-01T00:00:00Z' and time<'2018-05-01T00:00:00Z'"
var HKDL_Add_Point bool = true

var TimeClauseMonth_HKDL []Interval = []Interval{
	{"2018-01-01T00:00:00Z", "2018-02-01T00:00:00Z"},
	{"2018-02-01T00:00:00Z", "2018-03-01T00:00:00Z"},
	{"2018-03-01T00:00:00Z", "2018-04-01T00:00:00Z"},
	{"2018-04-01T00:00:00Z", "2018-05-01T00:00:00Z"},
	{"2018-05-01T00:00:00Z", "2018-06-01T00:00:00Z"},
	{"2018-06-01T00:00:00Z", "2018-07-01T00:00:00Z"},
	{"2018-07-01T00:00:00Z", "2018-08-01T00:00:00Z"},
	{"2018-08-01T00:00:00Z", "2018-09-01T00:00:00Z"},
	{"2018-09-01T00:00:00Z", "2018-10-01T00:00:00Z"},
	{"2018-10-01T00:00:00Z", "2018-11-01T00:00:00Z"},
	{"2018-11-01T00:00:00Z", "2018-12-01T00:00:00Z"},
	{"2018-12-01T00:00:00Z", "2019-01-01T00:00:00Z"},
}

func (f BaseFunction) HKDL_GetChillerPlantChillerRunning() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HKDL_GetChillerPlantChillerRunning"
	HKDL_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_Chiller_Running"
	// newId := "Total_Chiller_Running"
	// newEquipmentName := "Chiller_Plant"
	var TimeClauseMonth_HKDL []Interval = []Interval{
		{"2017-12-31T15:00:00Z", "2018-12-31T16:00:00Z"},
	}
	for _, ele := range TimeClauseMonth_HKDL {
		timeClause := fmt.Sprintf("time>='%s' and time<'%s'", ele.starttime, ele.endTime)
		query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Water_Flowrate' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(15m) `, f.Measurement, timeClause)
		ts := *client.QueryTimeseriesNewNew(query, f.Database, f.Host, f.Port)
		ts.GroupTimeseries("FunctionType", true)
		// ts.SortTimeseries("FunctionType")
		ts.ApplyFunctions([]client.ApplySchema{{
			Func: func(f ...float64) float64 {
				if tool.AllNan(f) {
					return math.NaN()
				}
				return tool.SumListStatusNew(tool.GetNonNan(f), 20.0)
			}, GroupByValue: map[string]string{"FunctionType": newFunctionType},
			NewGroupByKey: "id"}}...)
		ts.UnGroupTimeseries(true)
		points := ts.GenerateInfluxWriteSchema("hkdl", []string{"EquipmentName", "FunctionType", "id"}, 1, map[string]string{})
		client.InfluxdbWritePointsNew(url, f.Database, points, 1000)
		fmt.Println("endd")
	}
	HKDL_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HKDL_GetChillerPlantChillerEnergy() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HKDL_GetChillerPlantChillerEnergy"
	HKDL_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_Chiller_Energy"
	// newEquipmentName := "Chiller_Plant"
	// newId := "Total_Chiller_Energy"
	var TimeClauseMonth_HKDL []Interval = []Interval{
		{"2017-12-31T15:00:00Z", "2018-12-31T16:00:00Z"},
	}
	for _, ele := range TimeClauseMonth_HKDL {
		timeClause := fmt.Sprintf("time>='%s' and time<'%s'", ele.starttime, ele.endTime)
		query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(15m)`, f.Measurement, timeClause)
		ts := *client.QueryTimeseriesNewNew(query, f.Database, f.Host, f.Port)
		ts.GroupTimeseries("FunctionType", true)
		ts.SortTimeseries("EquipmentName")
		ts.ApplyFunctions([]client.ApplySchema{{
			Func: func(f ...float64) float64 {
				if tool.AllNan(f) {
					return math.NaN()
				}
				return tool.SumList(tool.GetNonNan(f))
			}, GroupByValue: map[string]string{"FunctionType": newFunctionType},
			NewGroupByKey: "id"}}...)
		ts.UnGroupTimeseries(true)
		points := ts.GenerateInfluxWriteSchema("hkdl", []string{"EquipmentName", "FunctionType", "id"}, 1, map[string]string{})
		client.InfluxdbWritePointsNew(url, f.Database, points, 1000)
		fmt.Println("endd")
	}

	HKDL_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HKDL_GetChillerPlantCoolingLoad() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HKDL_GetChillerPlantCoolingLoad"
	HKDL_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Cooling_Load"
	// newId := "Total_Chiller_Plant_Cooling_Load"
	newEquipmentName := "Chiller_Plant"
	var TimeClauseMonth_HKDL []Interval = []Interval{
		{"2018-12-30T15:00:00Z", "2018-12-31T16:00:00Z"},
	}
	for _, ele := range TimeClauseMonth_HKDL {
		timeClause := fmt.Sprintf("time>='%s' and time<'%s'", ele.starttime, ele.endTime)
		query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Water_Flowrate') AND
			%s GROUP BY EquipmentName, FunctionType, id, time(15m)`, f.Measurement, timeClause)
		ts := *client.QueryTimeseriesNewNew(query, f.Database, f.Host, f.Port)
		ts.GroupTimeseries("EquipmentName", true)
		ts.SortTimeseries("FunctionType")
		ts.ApplyFunctions([]client.ApplySchema{{
			Func: func(f ...float64) float64 {
				if tool.ContainNaN(f) || len(f) < 3 {
					return math.NaN()
				} else if f[2] > 10 && (f[0]-f[1]) > 0 {
					return (f[0] - f[1]) * 4.2 * f[2]
				} else {
					return 0
				}
			}, GroupByValue: map[string]string{"FunctionType": newFunctionType},
			NewGroupByKey: "id"}}...)
		ts.UnGroupTimeseries(true)
		ts.GroupTimeseries("FunctionType", true)
		ts.SortTimeseries("EquipmentName")
		ts.ApplyFunctions([]client.ApplySchema{{
			Func: func(f ...float64) float64 {
				if tool.AllNan(f) {
					return math.NaN()
				}
				return tool.SumList(tool.GetNonNan(f))
			}, GroupByValue: map[string]string{"EquipmentName": newEquipmentName},
			NewGroupByKey: "id", First: true}}...)
		points := ts.GenerateInfluxWriteSchema("hkdl", []string{"EquipmentName", "FunctionType", "id"}, 1, map[string]string{})
		fmt.Println(points[:10])
		client.InfluxdbWritePointsNew(url, f.Database, points, 1000)
		fmt.Println("endd")
	}

	HKDL_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HKDL_GetChillerPlantCoP() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HKDL_GetChillerPlantCoP"
	HKDL_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_CoP"
	newId := "Overall_Chiller_Plant_CoP"
	for _, ele := range TimeClauseMonth_HKDL {
		timeClause := fmt.Sprintf("time>='%s' and time<'%s'", ele.starttime, ele.endTime)
		query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Cooling_Load' OR
			"FunctionType"='Chiller_Plant_Total_Chiller_Energy') AND
			%s GROUP BY EquipmentName, FunctionType, id, time(15m)`, f.Measurement, timeClause)
		dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
		HKDL_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
		if len(dfGroup) == 0 {
			HKDL_Logger.Log(logging.LogError, "function %s: No data", name)
			return nil
		}
		wg := sync.WaitGroup{}
		wg.Add(len(dfGroup))
		for _, ele := range dfGroup {
			df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
				if tool.AllNan(f) {
					return math.NaN()
				}
				if len(f) < 2 {
					return math.NaN()
				}
				if f[0] > 1000 && f[1] > 50 {
					return f[0] / f[1]
				} else if tool.ContainNaN(f) {
					return math.NaN()
				} else {
					return 0
				}
			}, []int{1, 2}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
			HKDL_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
			go func(query string, database string, measurement string,
				EquipmentName string, FunctionType string, id string,
				df dataframe.DataFrame, startIndex int) {
				err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
				if err != nil {
					HKDL_Logger.Log(logging.LogError, "function %s error: %v", name, err)
				}
				if HKDL_Add_Point {
					err = client.AddClientPoint(fmt.Sprintf("neo4j://%s:%v", f.Neo4j_Host, f.Neo4j_Port), f.Neo4j_Username, f.Neo4j_Password,
						f.Database, f.Measurement, client.TaggingPoint{
							BMS_id:     id,
							PointName:  id,
							System:     "HVAC_System",
							SubSystem:  "Water_System",
							DeviceType: "Chiller_Plant",
							DeviceName: EquipmentName,
							PointType:  FunctionType,
							Location:   "Building",
							Level:      "HKDL",
							ClassType:  "Class",
							Interval:   "20T",
							Unit:       "None",
						}, []string{Calculated}...)
					if err != nil {
						HKDL_Logger.Log(logging.LogError, "function %s error: %v", name, err)
					}
				}
				wg.Done()
			}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
		}
		wg.Wait()
	}

	HKDL_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HKDL_GetChillerPlantWetBulb() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HKDL_GetChillerPlantWetBulb"
	HKDL_Logger.Log(logging.LogInfo, "START function %s", name)
	// newEquipmentName := "Chiller_Plant"
	newFunctionType := "Chiller_Plant_Outdoor_Wet_Bulb"
	var TimeClauseMonth_HKDL []Interval = []Interval{
		{"2017-12-31T15:00:00Z", "2018-12-31T16:00:00Z"},
	}
	for _, ele := range TimeClauseMonth_HKDL {
		timeClause := fmt.Sprintf("time>='%s' and time<'%s'", ele.starttime, ele.endTime)
		query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
		WHERE ("FunctionType"='Chiller_Plant_Outdoor_Dry_Bulb' OR
		"FunctionType"='Chiller_Plant_Outdoor_Humidity') AND
		%s GROUP BY EquipmentName, FunctionType, id, time(15m)`, f.Measurement, timeClause)
		ts := *client.QueryTimeseriesNewNew(query, f.Database, f.Host, f.Port)
		ts.GroupTimeseries("EquipmentName", true)
		ts.SortTimeseries("FunctionType")
		ts.ApplyFunctions([]client.ApplySchema{{
			Func: func(f ...float64) float64 {
				if tool.ContainNaN(f) || len(f) < 2 {
					return math.NaN()
				} else {
					return (f[0]*math.Atan(0.151977*math.Sqrt(f[1]+8.313659)) +
						math.Atan(f[0]+f[1]) - math.Atan(f[1]-1.676331) +
						0.00391838*math.Pow(f[1], 3/2)*math.Atan(0.023101*f[1]) -
						4.686035)
				}
			}, GroupByValue: map[string]string{"FunctionType": newFunctionType},
			NewGroupByKey: "id"}}...)
		ts.UnGroupTimeseries(true)
		points := ts.GenerateInfluxWriteSchema("hkdl", []string{"EquipmentName", "FunctionType", "id"}, 1, map[string]string{})
		client.InfluxdbWritePointsNew(url, f.Database, points, 1000)
		fmt.Println("endd")
	}
	HKDL_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HKDL_GetChillerCL() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HKDL_GetChillerCL"
	HKDL_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Cooling_Load"
	var TimeClauseMonth_HKDL []Interval = []Interval{
		{"2018-12-31T14:00:00Z", "2018-12-31T16:00:00Z"},
	}
	for _, ele := range TimeClauseMonth_HKDL {
		timeClause := fmt.Sprintf("time>='%s' and time<'%s'", ele.starttime, ele.endTime)
		query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Water_Flowrate') AND
			%s GROUP BY EquipmentName, FunctionType, id, time(15m)`, f.Measurement, timeClause)
		ts := *client.QueryTimeseriesNewNew(query, f.Database, f.Host, f.Port)
		ts.GroupTimeseries("EquipmentName", true)
		ts.SortTimeseries("FunctionType")
		ts.ApplyFunctions([]client.ApplySchema{{
			Func: func(f ...float64) float64 {
				if tool.AllNan(f) || len(f) < 3 {
					return math.NaN()
				} else if f[2] > 20 && (f[0]-f[1]) > 0 {
					return (f[0] - f[1]) * 4.2 * f[2]
				} else if tool.ContainNaN(f) {
					return math.NaN()
				} else {
					return 0
				}
			}, GroupByValue: map[string]string{"FunctionType": newFunctionType},
			NewGroupByKey: "id"}}...)
		ts.UnGroupTimeseries(true)
		ts.GenerateLocalTags(func(m map[string]string) map[string]string {
			m["id"] = m["EquipmentName"] + "_" + m["FunctionType"]
			m["prefername"] = m["id"]
			return m
		})
		points := ts.GenerateWriteSchema("hkdl", 1, map[string]string{"Block": "hkdl", "BuildingName": "hkdl"})
		time.Sleep(time.Hour)
		client.InfluxdbWritePointsNew(url, f.Database, points, 10000)
		fmt.Println("endd")
	}
	HKDL_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HKDL_GetChillerCoP() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HKDL_GetChillerCoP"
	HKDL_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_CoP"
	var TimeClauseMonth_HKDL []Interval = []Interval{
		{"2017-12-31T15:00:00Z", "2018-12-31T16:00:00Z"},
	}
	for _, ele := range TimeClauseMonth_HKDL {
		timeClause := fmt.Sprintf("time>='%s' and time<'%s'", ele.starttime, ele.endTime)
		query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Power_Sensor' OR
			"FunctionType"='Chiller_Water_Flowrate') AND
			%s GROUP BY EquipmentName, FunctionType, id, time(15m)`, f.Measurement, timeClause)
		ts := *client.QueryTimeseriesNewNew(query, f.Database, f.Host, f.Port)
		ts.GroupTimeseries("EquipmentName", true)
		ts.SortTimeseries("FunctionType")
		ts.ApplyFunctions([]client.ApplySchema{{
			Func: func(f ...float64) float64 {
				if tool.AllNan(f) || len(f) < 4 {
					return math.NaN()
				} else if f[2] > 20 && f[3] > 50 && (f[0]-f[1]) > 0 {
					return (f[0] - f[1]) * 4.2 * f[3] / f[2]
				} else if tool.ContainNaN(f) {
					return math.NaN()
				} else {
					return 0
				}
			}, GroupByValue: map[string]string{"FunctionType": newFunctionType},
			NewGroupByKey: "id"}}...)
		ts.UnGroupTimeseries(true)
		points := ts.GenerateInfluxWriteSchema("hkdl", []string{"EquipmentName", "FunctionType", "id"}, 1, map[string]string{})
		client.InfluxdbWritePointsNew(url, f.Database, points, 1000)
		fmt.Println("endd")
	}
	HKDL_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HKDL_GetChillerDeltaT() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HKDL_GetChillerDeltaT"
	// EquipType := "Chiller"
	HKDL_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Chilled_Delta_T"
	var TimeClauseMonth_HKDL []Interval = []Interval{
		{"2017-12-31T15:00:00Z", "2018-12-31T16:00:00Z"},
	}
	for _, ele := range TimeClauseMonth_HKDL {
		timeClause := fmt.Sprintf("time>='%s' and time<'%s'", ele.starttime, ele.endTime)
		query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor') AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(15m)`, f.Measurement, timeClause)
		ts := *client.QueryTimeseriesNewNew(query, f.Database, f.Host, f.Port)
		ts.GroupTimeseries("EquipmentName", true)
		ts.SortTimeseries("FunctionType")
		ts.ApplyFunctions([]client.ApplySchema{{
			Func: func(f ...float64) float64 {
				if len(f) < 2 {
					return math.NaN()
				}
				return f[0] - f[1]
			}, GroupByValue: map[string]string{"FunctionType": newFunctionType},
			NewGroupByKey: "id"}}...)
		ts.UnGroupTimeseries(true)
		// ts.GroupTimeseries("FunctionType", true)
		// ts.SortTimeseries("EquipmentName")
		// ts.ApplyFunctions([]client.ApplySchema{{
		// 	Func: func(f ...float64) float64 {
		// 		fmt.Println(f, ":ds")
		// 		if len(f) < 2 {
		// 			return math.NaN()
		// 		}
		// 		return f[0] - f[1]
		// 	}, GroupByValue: map[string]string{"EquipmentName": "Chiller_Plant"},
		// 	NewGroupByKey: "id"}}...)
		points := ts.GenerateInfluxWriteSchema("hkdl", []string{"EquipmentName", "FunctionType", "id"}, 1, map[string]string{})
		client.InfluxdbWritePointsNew(url, f.Database, points, 1000)
		fmt.Println("endd")
		return nil
	}
	HKDL_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}
