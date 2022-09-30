package functions

import (
	"fmt"
	"go-api-grapqhl/graph/client"
	logging "go-api-grapqhl/log"
	"go-api-grapqhl/tool"
	"math"
	"sync"

	"github.com/go-gota/gota/dataframe"
)

var HCity_1_Chiller = []int{1, 2, 3, 4, 5}
var HCity_1_CT = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}
var HCity_1_Logger = logging.StartLogger("log/HCity_1_LogFile.log")

var TimeClause_HCity string = "time>'2021-07-31T00:00:00Z' and time<'2021-10-31T00:00:00Z'"

var TimeClauseMonth_HCity []Interval = []Interval{
	{"2021-08-01T00:00:00Z", "2021-09-01T00:00:00Z"},
	{"2021-09-01T00:00:00Z", "2021-10-01T00:00:00Z"},
}

func (f BaseFunction) HCity1_GetChillerPlantChillerRunning() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HCity1_GetChillerPlantChillerRunning"
	HCity_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_Chiller_Running"
	newId := "Total_Chiller_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Capacity_Sensor' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(15m) `, f.Measurement, TimeClause_HCity)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	fmt.Println(dfGroup)
	HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		HCity_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumListStatusNew(tool.GetNonNan(f), 20.0)
		}, HCity_1_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint(fmt.Sprintf("neo4j://%s:%v", f.Neo4j_Host, f.Neo4j_Port), f.Neo4j_Username, f.Neo4j_Password,
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "Harbour City",
				ClassType:  "Class",
				Interval:   "15T",
				Unit:       "None",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	HCity_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HCity1_GetChillerPlantCoolingLoad() error {
	// TimeClause_HCity = "time>'2021-08-01T18:00:00Z' and time<'2021-08-08T00:00:00Z'"
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HCity1_GetChillerPlantCoolingLoad"
	HCity_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Cooling_Load"
	newId := "Total_Chiller_Plant_Cooling_Load"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Water_Flowrate') AND
			%s GROUP BY EquipmentName, FunctionType, id, time(15m)`, f.Measurement, TimeClause_HCity)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	dfGroup = client.ApplyFunctionDfGroup(dfGroup, func(f ...float64) float64 {
		if len(f) < 3 {
			return math.NaN()
		} else if f[2] > 50 && (f[0]-f[1]) > 0 {
			return (f[0] - f[1]) * 4.2 * f[2]
		} else if tool.ContainNaN(f) {
			return math.NaN()
		} else {
			return 0
		}
	}, "Chiller_Cooling_Load", []int{1, 2, 3}...)
	HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		HCity_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumList(tool.GetNonNan(f))
		}, HCity_1_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint(fmt.Sprintf("neo4j://%s:%v", f.Neo4j_Host, f.Neo4j_Port), f.Neo4j_Username, f.Neo4j_Password,
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "Harbour City",
				ClassType:  "Class",
				Interval:   "15T",
				Unit:       "kW",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	HCity_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HCity1_GetChillerPlantWetBulb() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HCity1_GetChillerPlantWetBulb"
	HCity_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newEquipmentName := "Chiller_Plant"
	newFunctionType := "Chiller_Plant_Outdoor_Wet_Bulb"
	newId := "Chiller_Plant_Outdoor_Wet_Bulb"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Outdoor_Dry_Bulb' OR
			"FunctionType"='Chiller_Plant_Outdoor_Humidity') AND
			%s GROUP BY EquipmentName, FunctionType, id, time(15m)`, f.Measurement, TimeClause_HCity)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		HCity_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 2 {
				return math.NaN()
			}
			if tool.ContainNaN(f) {
				return math.NaN()
			} else {
				return (f[0]*math.Atan(0.151977*math.Sqrt(f[1]+8.313659)) +
					math.Atan(f[0]+f[1]) - math.Atan(f[1]-1.676331) +
					0.00391838*math.Pow(f[1], 3/2)*math.Atan(0.023101*f[1]) -
					4.686035)
			}
		}, []int{1, 2}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint(fmt.Sprintf("neo4j://%s:%v", f.Neo4j_Host, f.Neo4j_Port), f.Neo4j_Username, f.Neo4j_Password,
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "Harbour City",
				ClassType:  "Class",
				Interval:   "15T",
				Unit:       "°C",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	HCity_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HCity1_GetChillerEnergy1Hour() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HCity1_GetChillerEnergy1Hour"
	HCity_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Power_Sensor(Calculated)(60m)"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_HCity)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		HCity_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 1 {
				return math.NaN()
			}
			return f[0]
		}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				HCity_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			err = client.AddClientPoint(fmt.Sprintf("neo4j://%s:%v", f.Neo4j_Host, f.Neo4j_Port), f.Neo4j_Username, f.Neo4j_Password,
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "Harbour City",
					ClassType:  "Electrical_Class",
					Interval:   "60T",
					Unit:       "kW",
				}, []string{Calculated}...)
			if err != nil {
				HCity_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s_%s", ele.EquipmentName, newFunctionType, "(60T)"), df, 1)
	}
	wg.Wait()
	HCity_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HCity1_GetChillerEnergy1Day() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HCity1_GetChillerEnergy1Day"
	HCity_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Power_Sensor(Calculated)(1d)"
	query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(1d)`, f.Measurement, TimeClause_HCity)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		HCity_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 1 {
				return math.NaN()
			}
			return f[0] / 4
		}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				HCity_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			err = client.AddClientPoint(fmt.Sprintf("neo4j://%s:%v", f.Neo4j_Host, f.Neo4j_Port), f.Neo4j_Username, f.Neo4j_Password,
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "Harbour City",
					ClassType:  "Electrical_Class",
					Interval:   "1d",
					Unit:       "kW",
				}, []string{Calculated}...)
			if err != nil {
				HCity_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s_%s", ele.EquipmentName, newFunctionType, "(1d)"), df, 1)
	}
	wg.Wait()
	HCity_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HCity1_GetChillerEnergy1Month() error {
	for _, ele := range timeClauseMonth {
		timeClause = fmt.Sprintf("time>'%s' and time<'%s'", ele.starttime, ele.endTime)
		url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
		name := "HCity1_GetChillerEnergy1Month"
		HCity_1_Logger.Log(logging.LogInfo, "START function %s", name)
		newFunctionType := "Chiller_Power_Sensor(Calculated)(1M)"
		query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			( %s ) 
			GROUP BY EquipmentName, FunctionType, id`, f.Measurement, timeClause)
		dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
		HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
		if len(dfGroup) == 0 {
			HCity_1_Logger.Log(logging.LogError, "function %s: No data", name)
			return nil
		}
		wg := sync.WaitGroup{}
		wg.Add(len(dfGroup))
		for _, ele := range dfGroup {
			df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
				if len(f) < 1 {
					return math.NaN()
				}
				return f[0] / 4
			}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
			HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
			go func(query string, database string, measurement string,
				EquipmentName string, FunctionType string, id string,
				df dataframe.DataFrame, startIndex int) {
				err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
				if err != nil {
					HCity_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
				}
				err = client.AddClientPoint(fmt.Sprintf("neo4j://%s:%v", f.Neo4j_Host, f.Neo4j_Port), f.Neo4j_Username, f.Neo4j_Password,
					f.Database, f.Measurement, client.TaggingPoint{
						BMS_id:     id,
						PointName:  id,
						System:     "HVAC_System",
						SubSystem:  "Water_System",
						DeviceType: "Chiller",
						DeviceName: EquipmentName,
						PointType:  newFunctionType,
						Location:   "Building",
						Level:      "Harbour City",
						ClassType:  "Electrical_Class",
						Interval:   "1M",
						Unit:       "Ton",
					}, []string{Calculated}...)
				if err != nil {
					HCity_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
				}
				wg.Done()
			}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s_%s", ele.EquipmentName, newFunctionType, "(1M)"), df, 0)
		}
		wg.Wait()
		HCity_1_Logger.Log(logging.LogInfo, "END function %s", name)
	}
	return nil
}

func (f BaseFunction) HCity1_GetChillerCL() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HCity1_GetChillerCL"
	HCity_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Cooling_Load"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Water_Flowrate') AND
			%s GROUP BY EquipmentName, FunctionType, id, time(15m)`, f.Measurement, TimeClause_HCity)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		HCity_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 3 {
				return math.NaN()
			}
			if f[2] > 50 && (f[0]-f[1]) > 0 {
				return (f[0] - f[1]) * 4.2 * f[2]
			} else if tool.ContainNaN(f) {
				return math.NaN()
			} else {
				return 0
			}
		}, []int{1, 2, 3}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				HCity_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			err = client.AddClientPoint(fmt.Sprintf("neo4j://%s:%v", f.Neo4j_Host, f.Neo4j_Port), f.Neo4j_Username, f.Neo4j_Password,
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "Harbour City",
					ClassType:  "Class",
					Interval:   "15T",
					Unit:       "kW",
				}, []string{Calculated}...)
			if err != nil {
				HCity_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), df, 1)
	}
	wg.Wait()
	HCity_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HCity1_GetChillerCoP() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HCity1_GetChillerCoP"
	HCity_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_CoP"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Power_Sensor' OR
			"FunctionType"='Chiller_Water_Flowrate') AND
			%s GROUP BY EquipmentName, FunctionType, id, time(15m)`, f.Measurement, TimeClause_HCity)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		HCity_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 4 {
				return math.NaN()
			}
			if f[2] > 50 && f[3] > 50 && (f[0]-f[1]) > 0 {
				return (f[0] - f[1]) * 4.2 * f[3] / f[2]
			} else if tool.ContainNaN(f) {
				return math.NaN()
			} else {
				return 0
			}
		}, HCity_1_Chiller...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				HCity_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			err = client.AddClientPoint(fmt.Sprintf("neo4j://%s:%v", f.Neo4j_Host, f.Neo4j_Port), f.Neo4j_Username, f.Neo4j_Password,
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "Harbour City",
					ClassType:  "Class",
					Interval:   "15T",
					Unit:       "None",
				}, []string{Calculated}...)
			if err != nil {
				HCity_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), df, 1)
	}
	wg.Wait()
	HCity_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) HCity1_GetChillerDeltaT() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "HCity1_GetChillerDeltaT"
	HCity_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_delta_T"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor') AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(15m)`, f.Measurement, TimeClause_HCity)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		HCity_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 2 {
				return math.NaN()
			}
			return f[0] - f[1]
		}, []int{1, 2}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		HCity_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				HCity_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			err = client.AddClientPoint(fmt.Sprintf("neo4j://%s:%v", f.Neo4j_Host, f.Neo4j_Port), f.Neo4j_Username, f.Neo4j_Password,
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "Harbour City",
					ClassType:  "Class",
					Interval:   "15T",
					Unit:       "Ton",
				}, []string{Calculated}...)
			if err != nil {
				HCity_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), df, 1)
	}
	wg.Wait()
	HCity_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}