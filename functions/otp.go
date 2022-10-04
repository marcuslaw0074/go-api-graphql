package functions

import (
	"fmt"
	"go-api-grapqhl/graph/client"
	logging "go-api-grapqhl/log"
	"go-api-grapqhl/tool"
	"math"
	// "sync"
	"time"

	// "github.com/go-gota/gota/dataframe"
)

var OTP_Chiller = []int{1, 2, 3, 4, 5, 6, 7, 8}
var OTP_CT = []int{1, 2, 3, 4, 5, 6, 7}
var OTP_Logger = logging.StartLogger("log/HKDL_LogFile.log")

var TimeClause_OTP string = "time>='2022-10-01T00:00:00Z' and time<'2022-10-03T00:00:00Z'"



func (f BaseFunction) OTP_GetChillerPlantCTRunning() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "OTP_GetChillerPlantCTRunning"
	OTP_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_CT_Running"
	newId := "Total_Chiller_Running"
	newEquipmentName := "Chiller_Plant"
	TimeClauseMonth_OTP := []Interval{
		{"now()-60m", "now()"},
	}
	for _, ele := range TimeClauseMonth_OTP {
		timeClause := fmt.Sprintf("time>=%s and time<%s", ele.starttime, ele.endTime)
		query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='CT_VFD_Speed_Status' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(20m) `, f.Measurement, timeClause)
		ts := *client.QueryTimeseriesNewNew(query, f.Database, f.Host, f.Port)
		ts.GroupTimeseries("FunctionType", true)
		ts.ApplyFunctions([]client.ApplySchema{{
			Func: func(f ...float64) float64 {
				if tool.AllNan(f) {
					return math.NaN()
				}
				return tool.SumListStatusNew(tool.GetNonNan(f), 20.0)
			}, GroupByValue: map[string]string{"FunctionType": newFunctionType},
			NewGroupByKey: "id"}}...)
		ts.UnGroupTimeseries(true)
		points := ts.GenerateWriteSchema("hkdl", 1, map[string]string{
			"Block": "hkdl", 
			"BuildingName": "hkdl", 
			"EquipmentName": newEquipmentName,
			"id": newId,
			"prefername": newId,
		})
		fmt.Println(points)
		time.Sleep(time.Hour)
		client.InfluxdbWritePointsNew(url, f.Database, points, 10000)
		fmt.Println("endd")
	}
	OTP_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) OTP_GetChillerPlantChillerRunning() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "OTP_GetChillerPlantCTRunning"
	OTP_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_Chiller_Running"
	newId := "Total_Chiller_Running"
	newEquipmentName := "Chiller_Plant"
	TimeClauseMonth_OTP := []Interval{
		{"now()-60m", "now()"},
	}
	for _, ele := range TimeClauseMonth_OTP {
		timeClause := fmt.Sprintf("time>=%s and time<%s", ele.starttime, ele.endTime)
		query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='CT_VFD_Speed_Status' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(20m) `, f.Measurement, timeClause)
		ts := *client.QueryTimeseriesNewNew(query, f.Database, f.Host, f.Port)
		ts.GroupTimeseries("FunctionType", true)
		ts.ApplyFunctions([]client.ApplySchema{{
			Func: func(f ...float64) float64 {
				if tool.AllNan(f) {
					return math.NaN()
				}
				return tool.SumListStatusNew(tool.GetNonNan(f), 20.0)
			}, GroupByValue: map[string]string{"FunctionType": newFunctionType},
			NewGroupByKey: "id"}}...)
		ts.UnGroupTimeseries(true)
		points := ts.GenerateWriteSchema("hkdl", 1, map[string]string{
			"Block": "hkdl", 
			"BuildingName": "hkdl", 
			"EquipmentName": newEquipmentName,
			"id": newId,
			"prefername": newId,
		})
		fmt.Println(points)
		time.Sleep(time.Hour)
		client.InfluxdbWritePointsNew(url, f.Database, points, 10000)
		fmt.Println("endd")
	}
	OTP_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}