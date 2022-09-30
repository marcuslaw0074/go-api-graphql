package functions

import (
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"go-api-grapqhl/graph/client"
	logging "go-api-grapqhl/log"
	"go-api-grapqhl/tool"
	"math"
	"sync"
)

var Sands_Casino_Chiller = []int{1, 2, 3, 4, 5}
var Sands_Casino_CT = []int{1, 2, 3, 4, 5}
var Sands_1_Logger = logging.StartLogger("log/Sands_Casino_LogFile.log")

var TimeClause_Sands string = "time>='2022-05-01T00:00:00Z' and time<='2022-07-01T00:00:00Z'"

var TimeClauseMonth_Sands []Interval = []Interval{
	{"2022-05-01T00:00:00Z", "2022-06-01T00:00:00Z"},
	{"2022-06-01T00:00:00Z", "2022-07-01T00:00:00Z"},
}

func (f BaseFunction) Sands_GetChillerPlantChillerRunning() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantChillerRunning"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_Chiller_Running"
	newId := "Total_Chiller_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM "%s" 
			WHERE "FunctionType"='Chiller_Capacity_Sensor' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m) `, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	fmt.Println(dfGroup)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumListStatusNew(tool.GetNonNan(f), 20.0)
		}, HCity_1_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
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
				Level:      "CASINO",
				ClassType:  "Class",
				Interval:   "60T",
				Unit:       "None",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerPlantChillerEnergy() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantChillerEnergy"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_Chiller_Energy"
	newEquipmentName := "Chiller_Plant"
	newId := "Total_Chiller_Energy"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumList(tool.GetNonNan(f))
		}, Sands_Casino_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
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
				Level:      "CASINO",
				ClassType:  "Electrical_Class",
				Interval:   "60T",
				Unit:       "kW",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerPlantCoolingLoad() error {
	// timeClause = "time>'2021-08-01T18:00:00Z' and time<'2021-08-08T00:00:00Z'"
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantCoolingLoad"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Cooling_Load"
	newId := "Total_Chiller_Plant_Cooling_Load"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Water_Flowrate') AND
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
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
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumList(tool.GetNonNan(f))
		}, Sands_Casino_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
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
				Level:      "CASINO",
				ClassType:  "Class",
				Interval:   "60T",
				Unit:       "kW",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerPlantCoP() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantCoP"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_CoP"
	newId := "Overall_Chiller_Plant_CoP"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Cooling_Load' OR
			"FunctionType"='Chiller_Plant_Total_Energy') AND
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
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
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
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
					Level:      "CASINO",
					ClassType:  "Class",
					Interval:   "60T",
					Unit:       "None",
				}, []string{Calculated}...)
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
	}
	wg.Wait()
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

// not yet done
func (f BaseFunction) Sands_GetChillerPlantWetBulb() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantWetBulb"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newEquipmentName := "Chiller_Plant"
	newFunctionType := "Chiller_Plant_Outdoor_Wet_Bulb"
	newId := "Chiller_Plant_Outdoor_Wet_Bulb"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Outdoor_Dry_Bulb' OR
			"FunctionType"='Chiller_Plant_Outdoor_Humidity') AND
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
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
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
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
				Level:      "CASINO",
				ClassType:  "Class",
				Interval:   "60T",
				Unit:       "°C",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerPlantCTRunning() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantCTRunning"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_Cooling_Tower_Running"
	newId := "Total_Cooling_Tower_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='CT_Energy' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	df, err := tool.ConcatDataframe(dfGroup)
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumListStatusNew(tool.GetNonNan(f), 20)
		}, Sands_Casino_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
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
				Level:      "CASINO",
				ClassType:  "Class",
				Interval:   "60T",
				Unit:       "None",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerPlantCHWPRunning() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantPCHWPRunning"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newId := "Total_CHWP_Running"
	newFunctionType := "Chiller_Plant_Total_CHWP_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chilled_Water_Pump_Energy' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	df, err := tool.ConcatDataframe(dfGroup)
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumListStatusNew(tool.GetNonNan(f), 20)
		}, Sands_Casino_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
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
				Level:      "CASINO",
				ClassType:  "Class",
				Interval:   "60T",
				Unit:       "None",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerPlantCDWPRunning() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantCDWPRunning"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newId := "Total_CDWP_Running"
	newFunctionType := "Chiller_Plant_Total_CDWP_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Condenser_Water_Pump_Energy' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	df, err := tool.ConcatDataframe(dfGroup)
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumListStatusNew(tool.GetNonNan(f), 20)
		}, Sands_Casino_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
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
				Level:      "CASINO",
				ClassType:  "Class",
				Interval:   "60T",
				Unit:       "None",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerPlantCTEnergy() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantCTEnergy"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_CT_Energy"
	newId := "Total_CT_Energy"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='CT_Energy' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumList(tool.GetNonNan(f))
		}, Sands_Casino_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
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
				Level:      "CASINO",
				ClassType:  "Electrical_Class",
				Interval:   "60T",
				Unit:       "kW",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerPlantCHWPEnergy() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantCHWPEnergy"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_CHWP_Energy"
	newId := "Total_CHWP_Energy"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chilled_Water_Pump_Energy' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumList(tool.GetNonNan(f))
		}, Sands_Casino_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
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
				Level:      "CASINO",
				ClassType:  "Electrical_Class",
				Interval:   "60T",
				Unit:       "kW",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerPlantCDWPEnergy() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantCDWPEnergy"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_CDWP_Energy"
	newId := "Total_CDWP_Energy"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Condenser_Water_Pump_Energy' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumList(tool.GetNonNan(f))
		}, Sands_Casino_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
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
				Level:      "CASINO",
				ClassType:  "Electrical_Class",
				Interval:   "60T",
				Unit:       "kW",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerPlantTotalEnergy() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantTotalEnergy"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_Energy"
	newId := "Total_Energy"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Total_CT_Energy' OR
			"FunctionType"='Chiller_Plant_Total_Chiller_Energy' OR
			"FunctionType"='Chiller_Plant_Total_CHWP_Energy' OR
			"FunctionType"='Chiller_Plant_Total_CDWP_Energy' 
			) AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumList(tool.GetNonNan(f))
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
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
					Level:      "CASINO",
					ClassType:  "Electrical_Class",
					Interval:   "60T",
					Unit:       "kW",
				}, []string{Calculated}...)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
	}
	wg.Wait()
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerEnergy1Hour() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerEnergy1Hour"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Power_Sensor(Calculated)(60m)"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	// wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 1 {
				return math.NaN()
			}
			return f[0]
		}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		wg.Add(1)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
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
					Level:      "CASINO",
					ClassType:  "Electrical_Class",
					Interval:   "60T",
					Unit:       "kW",
				}, []string{Calculated}...)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s_%s", ele.EquipmentName, newFunctionType, "(60T)"), df, 1)
		wg.Wait()
	}
	// wg.Wait()
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerEnergy1Day() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerEnergy1Day"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Power_Sensor(Calculated)(1d)"
	query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(1d)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	// wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 1 {
				return math.NaN()
			}
			return f[0]
		}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		wg.Add(1)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
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
					Level:      "CASINO",
					ClassType:  "Electrical_Class",
					Interval:   "1d",
					Unit:       "kW",
				}, []string{Calculated}...)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s_%s", ele.EquipmentName, newFunctionType, "(1d)"), df, 1)
		wg.Wait()
	}
	// wg.Wait()
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerEnergy1Month() error {
	for _, ele := range TimeClauseMonth_Sands {
		timeClause = fmt.Sprintf("time>='%s' and time<'%s'", ele.starttime, ele.endTime)
		url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
		name := "Sands_GetChillerEnergy1Month"
		Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
		newFunctionType := "Chiller_Power_Sensor(Calculated)(1M)"
		query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			( %s ) 
			GROUP BY EquipmentName, FunctionType, id`, f.Measurement, timeClause)
		dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
		if len(dfGroup) == 0 {
			Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
			return nil
		}
		wg := sync.WaitGroup{}
		// wg.Add(len(dfGroup))
		for _, ele := range dfGroup {
			df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
				if len(f) < 1 {
					return math.NaN()
				}
				return f[0]
			}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
			Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
			wg.Add(1)
			go func(query string, database string, measurement string,
				EquipmentName string, FunctionType string, id string,
				df dataframe.DataFrame, startIndex int) {
				fmt.Println(df)
				err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
				if err != nil {
					Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
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
						Level:      "CASINO",
						ClassType:  "Electrical_Class",
						Interval:   "1M",
						Unit:       "kW",
					}, []string{Calculated}...)
				if err != nil {
					Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
				}
				wg.Done()
			}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s_%s", ele.EquipmentName, newFunctionType, "(1M)"), df, 0)
			wg.Wait()
		}
		// wg.Wait()
		Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	}
	return nil
}

// no need to run
func (f BaseFunction) Sands_GetChillerCL() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerCL"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Cooling_Load(Calculated)"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Water_Flowrate') AND
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	// wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 3 {
				return math.NaN()
			}
			if f[2] > 500 && (f[0]-f[1]) > 0 {
				return (f[0] - f[1]) * 4.2 * 0.0631 * f[2]
			} else if tool.ContainNaN(f) {
				return math.NaN()
			} else {
				return 0
			}
		}, []int{1, 2, 3}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		wg.Add(1)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
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
					Level:      "CASINO",
					ClassType:  "Class",
					Interval:   "60T",
					Unit:       "kW",
				}, []string{Calculated}...)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), df, 1)
		wg.Wait()
	}
	// wg.Wait()
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerDeltaT() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerDeltaT"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_delta_T"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor') AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	// wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 2 {
				return math.NaN()
			}
			return f[0] - f[1]
		}, []int{1, 2}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		wg.Add(1)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
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
					Level:      "CASINO",
					ClassType:  "Class",
					Interval:   "60T",
					Unit:       "°C",
				}, []string{Calculated}...)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), df, 1)
		wg.Wait()
	}
	// wg.Wait()
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerCoP() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerCoP"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_CoP"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Power_Sensor' OR
			"FunctionType"='Chiller_Water_Flowrate') AND
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	// wg.Add(len(dfGroup))
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
		}, Sands_Casino_Chiller...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		wg.Add(1)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
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
					Level:      "CASINO",
					ClassType:  "Class",
					Interval:   "60T",
					Unit:       "None",
				}, []string{Calculated}...)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), df, 1)
		wg.Wait()
	}
	// wg.Wait()
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerPlantEnergy1Hour() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantEnergy1Hour"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_Chiller_Energy(Calculated)(60m)"
	newId := "Chiller_Plant_Chiller_Plant_Total_Chiller_Energy(Calculated)(60m)_(60T)"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Total_CT_Energy' OR
			"FunctionType"='Chiller_Plant_Total_Chiller_Energy' OR
			"FunctionType"='Chiller_Plant_Total_CHWP_Energy' OR
			"FunctionType"='Chiller_Plant_Total_CDWP_Energy' ) AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	df, err := tool.ConcatDataframe(dfGroup)
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumList(tool.GetNonNan(f))
		}, Sands_Casino_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
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
				Level:      "CASINO",
				ClassType:  "Electrical_Class",
				Interval:   "60T",
				Unit:       "kW",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerPlantEnergy1Day() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantEnergy1Day"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_Chiller_Energy(Calculated)(1d)"
	newId := "Chiller_Plant_Chiller_Plant_Total_Chiller_Energy(Calculated)(1d)_(1d)"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE ( "FunctionType"='Chiller_Plant_Total_CT_Energy' OR
			"FunctionType"='Chiller_Plant_Total_Chiller_Energy' OR
			"FunctionType"='Chiller_Plant_Total_CHWP_Energy' OR
			"FunctionType"='Chiller_Plant_Total_CDWP_Energy' ) AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(1d)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	df, err := tool.ConcatDataframe(dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 4 {
				return math.NaN()
			}
			return tool.SumList(tool.GetNonNan(f))
		}, Sands_Casino_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
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
				Level:      "CASINO",
				ClassType:  "Electrical_Class",
				Interval:   "1d",
				Unit:       "kW",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetChillerPlantEnergy1Month() error {
	name := "Sands_GetChillerPlantEnergy1Month"
	for _, ele := range TimeClauseMonth_Sands {
		timeClause = fmt.Sprintf("time>='%s' and time<'%s'", ele.starttime, ele.endTime)
		url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
		Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
		newFunctionType := "Chiller_Plant_Total_Chiller_Energy(Calculated)(1M)"
		newId := "Chiller_Plant_Chiller_Plant_Total_Chiller_Energy(Calculated)(1M)_(1M)"
		newEquipmentName := "Chiller_Plant"
		query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE ( "FunctionType"='Chiller_Plant_Total_CT_Energy' OR
			"FunctionType"='Chiller_Plant_Total_Chiller_Energy' OR
			"FunctionType"='Chiller_Plant_Total_CHWP_Energy' OR
			"FunctionType"='Chiller_Plant_Total_CDWP_Energy' ) AND 
			( %s ) 
			GROUP BY EquipmentName, FunctionType, id`, f.Measurement, timeClause)
		dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
		if len(dfGroup) == 0 {
			Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
			return nil
		}
		df, err := tool.ConcatDataframe(dfGroup)
		if err != nil {
			return err
		} else {
			df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
				if len(f) < 4 {
					return math.NaN()
				}
				return tool.SumList(tool.GetNonNan(f))
			}, Sands_Casino_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
			Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
			err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 0)
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
					Level:      "CASINO",
					ClassType:  "Electrical_Class",
					Interval:   "1M",
					Unit:       "kW",
				}, []string{Calculated}...)
			if err != nil {
				return err
			}
		}
	}

	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}

func (f BaseFunction) Sands_GetCTStatus() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetCTStatus"
	Sands_1_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "CT_Status"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='CT_Energy' AND
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	if len(dfGroup) == 0 {
		Sands_1_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	// wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumListStatusNew(tool.GetNonNan(f), 20)
		}, Utility_1_CT...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		Sands_1_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		wg.Add(1)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			err = client.AddClientPoint(fmt.Sprintf("neo4j://%s:%v", f.Neo4j_Host, f.Neo4j_Port), f.Neo4j_Username, f.Neo4j_Password,
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Cooling_Tower",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "CASINO",
					ClassType:  "Class",
					Interval:   "60T",
					Unit:       "None",
				}, []string{Calculated}...)
			if err != nil {
				Sands_1_Logger.Log(logging.LogError, "function %s error: %v", name, err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), df, 1)
		wg.Wait()
	}
	// wg.Wait()
	Sands_1_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}
