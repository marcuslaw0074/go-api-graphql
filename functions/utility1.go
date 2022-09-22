package functions

import (
	"fmt"
	"go-api-grapqhl/graph/client"
	"go-api-grapqhl/tool"
	"log"
	"math"
	"sync"

	"github.com/go-gota/gota/dataframe"
)

// == model uid 0
func (f BaseFunction) Utility1_GetChillerPlantChillerRunning() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantChillerRunning"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Plant_Total_Chiller_Running"
	newId := "Total_Chiller_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Status' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		log.Printf("function %s: No data", name)
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumListStatus(tool.GetNonNan(f))
		}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		fmt.Println(df)
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "UT1",
				ClassType:  "Class",
				Interval:   "20T",
				Unit:       "None",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	log.Printf("END function :%s", name)
	return nil
}

// == model uid 1
func (f BaseFunction) Utility1_GetChillerPlantChillerEnergy() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantChillerEnergy"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Plant_Total_Chiller_Energy"
	newEquipmentName := "Chiller_Plant"
	newId := "Total_Chiller_Energy"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		log.Printf("function %s: No data", name)
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumList(tool.GetNonNan(f))
		}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		fmt.Println(df)
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "UT1",
				ClassType:  "Electrical_Class",
				Interval:   "20T",
				Unit:       "kW",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	log.Printf("END function :%s", name)
	return nil
}

// == model uid 2
func (f BaseFunction) Utility1_GetChillerPlantCoolingLoad() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantCoolingLoad"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Plant_Cooling_Load"
	newId := "Total_Chiller_Plant_Cooling_Load"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Water_Flowrate') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	dfGroup = client.ApplyFunctionDfGroup(dfGroup, func(f ...float64) float64 {
		if len(f) < 3 {
			return math.NaN()
		} else if f[2] > 500 && (f[0]-f[1]) > 0 {
			return (f[0] - f[1]) * 4.2 * 0.0631 * f[2]
		} else if tool.ContainNaN(f) {
			return math.NaN()
		} else {
			return 0
		}
	}, "Chiller_Cooling_Load", []int{1, 2, 3}...)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		log.Printf("function %s: No data", name)
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumList(tool.GetNonNan(f))
		}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		fmt.Println(df)
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "UT1",
				ClassType:  "Class",
				Interval:   "20T",
				Unit:       "kW",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	log.Printf("END function :%s", name)
	return nil
}

// == model uid 3
func (f BaseFunction) Utility1_GetChillerPlantCoP() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantCoP"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Plant_CoP"
	newId := "Overall_Chiller_Plant_CoP"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Cooling_Load' OR
			"FunctionType"='Chiller_Plant_Total_Chiller_Energy') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
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
		fmt.Println(df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: EquipmentName,
					PointType:  FunctionType,
					Location:   "Building",
					Level:      "UT1",
					ClassType:  "Class",
					Interval:   "20T",
					Unit:       "None",
				}, []string{Calculated}...)
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}

// == model uid 4, no data in ut1 coz no functiontype
func (f BaseFunction) Utility1_GetChillerPlantDeltaT() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantDeltaT"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Plant_Delta_T"
	newId := "Overall_Chiller_Plant_Delta_T"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Plant_Chilled_Water_Supply_Temperature_Sensor') AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
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
		fmt.Println(df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: EquipmentName,
					PointType:  FunctionType,
					Location:   "Building",
					Level:      "UT1",
					ClassType:  "Class",
					Interval:   "20T",
					Unit:       "°C",
				}, []string{Calculated}...)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}

// == model uid 5
func (f BaseFunction) Utility1_GetChillerPlantWetBulb() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantWetBulb"
	log.Printf("START function :%s", name)
	newEquipmentName := "Chiller_Plant"
	newFunctionType := "Chiller_Plant_Outdoor_Wet_Bulb"
	newId := "Chiller_Plant_Outdoor_Wet_Bulb"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Outdoor_Dry_Bulb' OR
			"FunctionType"='Chiller_Plant_Outdoor_Humidity') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
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
		fmt.Println(df)
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "UT1",
				ClassType:  "Class",
				Interval:   "20T",
				Unit:       "°C",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	log.Printf("END function :%s", name)
	return nil
}

// == model uid 6
func (f BaseFunction) Utility1_GetChillerPlantCoP_kWPerTon() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantCoP_kWPerTon"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Plant_CoP(kW/Ton)"
	newId := "Overall_Chiller_Plant_CoP(kW/Ton)"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Cooling_Load' OR
			"FunctionType"='Chiller_Plant_Total_Chiller_Energy') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 2 {
				return math.NaN()
			}
			if f[0] > 1000 && f[1] > 50 {
				return f[1] / f[0] * 3.5169
			} else if tool.ContainNaN(f) {
				return math.NaN()
			} else {
				return 0
			}
		}, []int{1, 2}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		fmt.Println(df)
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "UT1",
				ClassType:  "Class",
				Interval:   "20T",
				Unit:       "kW/Ton",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	log.Printf("END function :%s", name)
	return nil
}

// == model uid 7
func (f BaseFunction) Utility1_GetChillerPlantCTRunning() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantCTRunning"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Plant_Total_Cooling_Tower_Running"
	newId := "Total_Cooling_Tower_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Cooling_Tower_Total_Status' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	df, err := tool.ConcatDataframe(dfGroup)
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			val := 0
			for _, ele := range f {
				if ele > 0.0 {
					val++
				}
			}
			return float64(val)
		}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		fmt.Println(df)
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "UT1",
				ClassType:  "Class",
				Interval:   "20T",
				Unit:       "None",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	log.Printf("END function :%s", name)
	return nil
}

// == model uid 8, not yet tested coz wrong tagging file
func (f BaseFunction) Utility1_GetChillerPlantPCHWPRunning() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantPCHWPRunning"
	log.Printf("START function :%s", name)
	newId := "Total_PCHWP_Running"
	newFunctionType := "Chiller_Plant_Total_PCHWP_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Primary_Chilled_Water_Pump_Status' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	df, err := tool.ConcatDataframe(dfGroup)
	fmt.Println(df)
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			val := 0
			for _, ele := range f {
				if ele == 1 {
					val++
				}
			}
			return float64(val)
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "UT1",
				ClassType:  "Class",
				Interval:   "20T",
				Unit:       "None",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	log.Printf("END function :%s", name)
	return nil
}

// == model uid 9
func (f BaseFunction) Utility1_GetChillerPlantSCHWPRunning() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantSCHWPRunning"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Plant_Total_SCHWP_Running"
	newId := "Total_SCHWP_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Secondary_Chilled_Water_Pump_Status' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	df, err := tool.ConcatDataframe(dfGroup)
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumListStatus(tool.GetNonNan(f))
		}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		fmt.Println(df)
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "UT1",
				ClassType:  "Class",
				Interval:   "20T",
				Unit:       "None",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	log.Printf("END function :%s", name)
	return nil
}

// == model uid 10
func (f BaseFunction) Utility1_GetChillerPlantCTEnergy() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantCTEnergy"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Plant_Total_CT_Energy"
	newId := "Total_CT_Energy"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Cooling_Tower_Total_Status' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		log.Printf("function %s: No data", name)
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumList(tool.GetNonNan(f)) * 11
		}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		fmt.Println(df)
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "UT1",
				ClassType:  "Electrical_Class",
				Interval:   "20T",
				Unit:       "kW",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	log.Printf("END function :%s", name)
	return nil
}

// == model uid 11
func (f BaseFunction) Utility1_GetChillerPlantTotalEnergy() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantTotalEnergy"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Plant_Total_Energy"
	newId := "Total_Energy"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Total_CT_Energy' OR
			"FunctionType"='Chiller_Plant_Total_Chiller_Energy') AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 2 {
				return math.NaN()
			}
			return f[0] + f[1]
		}, []int{1, 2}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: EquipmentName,
					PointType:  FunctionType,
					Location:   "Building",
					Level:      "UT1",
					ClassType:  "Electrical_Class",
					Interval:   "20T",
					Unit:       "kW",
				}, []string{Calculated}...)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}

// == model uid 12
func (f BaseFunction) Utility1_GetChillerPlantCoolingLoadTon() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantCoolingLoadTon"
	log.Printf("START function :%s", name)
	newEquipmentName := "Chiller_Plant"
	newFunctionType := "Chiller_Plant_Cooling_Load_Ton"
	newId := "Total_Chiller_Plant_Cooling_Load(Ton)"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Cooling_Load') AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 1 {
				return math.NaN()
			}
			return f[0] / 3.5169
		}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		fmt.Println(df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     newId,
					PointName:  newId,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: newEquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "UT1",
					ClassType:  "Class",
					Interval:   "20T",
					Unit:       "Ton",
				}, []string{Calculated}...)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}

// tested model functions
///////////////////////////////////////////////////////////////////////////////

// individual model uid 0
func (f BaseFunction) Utility1_GetChillerEnergy1Hour() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerEnergy1Hour"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Power_Sensor(Calculated)(60m)"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-360m GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
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
		fmt.Println(df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "UT1",
					ClassType:  "Electrical_Class",
					Interval:   "20T",
					Unit:       "Ton",
				}, []string{Calculated}...)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s_%s", ele.EquipmentName, newFunctionType, "(60T)"), df, 1)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}

// individual model uid 1
func (f BaseFunction) Utility1_GetChillerEnergy1Day() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerEnergy1Day"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Power_Sensor(Calculated)(1d)"
	query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-4d GROUP BY EquipmentName, FunctionType, id, time(1d)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 1 {
				return math.NaN()
			}
			return f[0] / 3
		}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		fmt.Println(df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "UT1",
					ClassType:  "Electrical_Class",
					Interval:   "20T",
					Unit:       "Ton",
				}, []string{Calculated}...)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s_%s", ele.EquipmentName, newFunctionType, "(1d)"), df, 1)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}

// individual model uid 2
func (f BaseFunction) Utility1_GetChillerEnergy1Month() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerEnergy1Month"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Power_Sensor(Calculated)(1M)"
	query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			( time>'%s' AND time<now() ) 
			GROUP BY EquipmentName, FunctionType, id`, f.Measurement, tool.GetCurrenttimeString())
	dfGroup := client.QueryDfGroup(query, f.Database)
	fmt.Println(dfGroup)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 1 {
				return math.NaN()
			}
			return f[0] / 1000 / 3
		}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "UT1",
					ClassType:  "Electrical_Class",
					Interval:   "20T",
					Unit:       "Ton",
				}, []string{Calculated}...)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s_%s", ele.EquipmentName, newFunctionType, "(1M)"), df, 0)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}

// individual model uid 3
func (f BaseFunction) Utility1_GetChillerCL() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerCL"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Cooling_Load"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Water_Flowrate') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
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
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "UT1",
					ClassType:  "Class",
					Interval:   "20T",
					Unit:       "Ton",
				}, []string{Calculated}...)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), df, 1)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}

// individual model uid 4
func (f BaseFunction) Utility1_GetChillerCoP() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerCoP"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_CoP"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Power_Sensor' OR
			"FunctionType"='Chiller_Water_Flowrate') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 4 {
				return math.NaN()
			}
			if f[2]/1000 > 50 && f[3] > 100 && (f[0]-f[1]) > 0 {
				return (f[0] - f[1]) * 4.2 * 0.0631 * f[3] / f[2] * 1000
			} else if tool.ContainNaN(f) {
				return math.NaN()
			} else {
				return 0
			}
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "UT1",
					ClassType:  "Class",
					Interval:   "20T",
					Unit:       "Ton",
				}, []string{Calculated}...)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), df, 1)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}

// individual model uid 5
func (f BaseFunction) Utility1_GetChillerDeltaT() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerDeltaT"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_delta_T"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor') AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
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
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "UT1",
					ClassType:  "Class",
					Interval:   "20T",
					Unit:       "Ton",
				}, []string{Calculated}...)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), df, 1)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}

// individual model uid 6
func (f BaseFunction) Utility1_GetChillerPlantEnergy1Hour() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantEnergy1Hour"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Plant_Total_Chiller_Energy(Calculated)(60m)"
	newId := "Chiller_Plant_Chiller_Plant_Total_Chiller_Energy(Calculated)(60m)_(60T)"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-240m GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
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
			return (f[0] + f[1] + f[2] + f[3]) / 1000
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "UT1",
				ClassType:  "Electrical_Class",
				Interval:   "20T",
				Unit:       "Ton",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	log.Printf("END function :%s", name)
	return nil
}

// individual model uid 7
func (f BaseFunction) Utility1_GetChillerPlantEnergy1Day() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantEnergy1Day"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Plant_Total_Chiller_Energy(Calculated)(1d)"
	newId := "Chiller_Plant_Chiller_Plant_Total_Chiller_Energy(Calculated)(1d)_(1d)"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-4d GROUP BY EquipmentName, FunctionType, id, time(1d)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	df, err := tool.ConcatDataframe(dfGroup)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 4 {
				return math.NaN()
			}
			return (f[0] + f[1] + f[2] + f[3]) / 1000 / 3
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "UT1",
				ClassType:  "Electrical_Class",
				Interval:   "20T",
				Unit:       "Ton",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	log.Printf("END function :%s", name)
	return nil
}

// individual model uid 8
func (f BaseFunction) Utility1_GetChillerPlantEnergy1Month() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerPlantEnergy1Month"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Plant_Total_Chiller_Energy(Calculated)(1M)"
	newId := "Chiller_Plant_Chiller_Plant_Total_Chiller_Energy(Calculated)(1M)_(1M)"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			( time>'%s' AND time<now() ) 
			GROUP BY EquipmentName, FunctionType, id`, f.Measurement, tool.GetCurrenttimeString())
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
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
			return (f[0] + f[1] + f[2] + f[3]) / 1000 / 3
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 0)
		if err != nil {
			return err
		}
		err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
			f.Database, f.Measurement, client.TaggingPoint{
				BMS_id:     newId,
				PointName:  newId,
				System:     "HVAC_System",
				SubSystem:  "Water_System",
				DeviceType: "Chiller_Plant",
				DeviceName: newEquipmentName,
				PointType:  newFunctionType,
				Location:   "Building",
				Level:      "UT1",
				ClassType:  "Electrical_Class",
				Interval:   "20T",
				Unit:       "Ton",
			}, []string{Calculated}...)
		if err != nil {
			return err
		}
	}
	log.Printf("END function :%s", name)
	return nil
}

// individual model uid 9
func (f BaseFunction) Utility1_GetChillerCoPkWPerTon() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerCoPkWPerTon"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_CoP(kW/ton)"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Power_Sensor' OR
			"FunctionType"='Chiller_Water_Flowrate') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 4 {
				return math.NaN()
			}
			if f[2]/1000 > 50 && f[3] > 100 && (f[0]-f[1]) > 0 {
				return (f[2] / 1000) / ((f[0] - f[1]) * 4.2 * 0.0631 * f[3]) * 3.5169
			} else if tool.ContainNaN(f) {
				return math.NaN()
			} else {
				return 0
			}
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "UT1",
					ClassType:  "Class",
					Interval:   "20T",
					Unit:       "Ton",
				}, []string{Calculated}...)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), df, 1)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}

// individual model uid 10
func (f BaseFunction) Utility1_GetCTStatus() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetCTStatus"
	log.Printf("START function :%s", name)
	newFunctionType := "Cooling_Tower_Total_Status"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Cooling_Tower_Status_01' OR
			"FunctionType"='Cooling_Tower_Status_02' OR 
			"FunctionType"='Cooling_Tower_Status_03' OR
			"FunctionType"='Cooling_Tower_Status_04') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumListStatus(tool.GetNonNan(f))
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		fmt.Println(df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "UT1",
					ClassType:  "Class",
					Interval:   "20T",
					Unit:       "Ton",
				}, []string{Calculated}...)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), df, 1)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

// individual energy from voltage and current
func (f BaseFunction) Utility1_GetChillerEnergy() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility1_GetChillerCoP"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Power_Sensor"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Current_Sensor_01' OR
			"FunctionType"='Chiller_Current_Sensor_02' OR 
			"FunctionType"='Chiller_Current_Sensor_03' OR
			"FunctionType"='Chiller_Voltage_Sensor_01' OR 
			"FunctionType"='Chiller_Voltage_Sensor_02' OR
			"FunctionType"='Chiller_Voltage_Sensor_03') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	mappingId := map[string]string{"CH15": "UT1_CH15_Input_Power", "CH16": "UT1_CH16_Input_Power", "CH17": "UT1_CH17_Input_Power"}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		if ele.Dataframe.Nrow() == 0 || (ele.EquipmentName != "CH15" && ele.EquipmentName != "CH16" && ele.EquipmentName != "CH17") {
			fmt.Printf("No need to run for Equipment: %s \n", ele.EquipmentName)
			continue
		}
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 6 {
				return math.NaN()
			} else if tool.ContainNaN(f) {
				return math.NaN()
			} else {
				return (f[0]*f[3] + f[1]*f[4] + f[2]*f[5]) / 3 * math.Sqrt(3) / 1000
			}
		}, []int{1, 2, 3, 4, 5, 6}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "UT1",
					ClassType:  "Class",
					Interval:   "20T",
					Unit:       "Ton",
				}, []string{Calculated}...)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, mappingId[ele.EquipmentName], df, 1)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}

// NEW individual model uid 11
func (f BaseFunction) Utility1_GetChillerCLTon() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Utility3_GetChillerCL"
	log.Printf("START function :%s", name)
	newFunctionType := "Chiller_Cooling_Load(Ton)"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Cooling_Load') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	if len(dfGroup) == 0 {
		log.Printf("function %s: No data", name)
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 1 {
				return math.NaN()
			}
			return f[0] / 3.5169
		}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(url, query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			err = client.AddClientPoint("neo4j://192.168.100.214:27687", "neo4j", "test",
				f.Database, f.Measurement, client.TaggingPoint{
					BMS_id:     id,
					PointName:  id,
					System:     "HVAC_System",
					SubSystem:  "Water_System",
					DeviceType: "Chiller_Plant",
					DeviceName: EquipmentName,
					PointType:  newFunctionType,
					Location:   "Building",
					Level:      "UT3",
					ClassType:  "Class",
					Interval:   "20T",
					Unit:       "Ton",
				}, []string{Calculated}...)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, fmt.Sprintf("%s_%s", ele.EquipmentName, newFunctionType), df, 1)
	}
	wg.Wait()
	log.Printf("END function :%s", name)
	return nil
}