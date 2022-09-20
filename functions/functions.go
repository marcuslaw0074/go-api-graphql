package functions

import (
	"fmt"
	"go-api-grapqhl/graph/client"
	"go-api-grapqhl/tool"
	"math"
	"sync"
	"github.com/go-gota/gota/dataframe"
)

type BaseFunction struct {
	Host        string
	Port        int
	Database    string
	Measurement string
}

// == model uid 0
func (f BaseFunction) GetChillerPlantChillerRunning() error {
	newFunctionType := "Chiller_Plant_Total_Chiller_Running"
	newId := "Total_Chiller_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	fmt.Println(dfGroup)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		fmt.Println("No data")
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 4 {
				return math.NaN()
			}
			if tool.ContainNaN(f) {
				return math.NaN()
			} else {
				val := 0
				for _, ele := range f {
					if ele/1000 > 50 {
						val++
					}
				}
				return float64(val)
			}
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		fmt.Println(df)
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

// == model uid 1
func (f BaseFunction) GetChillerPlantChillerEnergy() error {
	newFunctionType := "Chiller_Plant_Total_Chiller_Energy"
	newEquipmentName := "Chiller_Plant"
	newId := "Total_Chiller_Energy"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		fmt.Println("No data")
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return (f[0] + f[1] + f[2] + f[3]) / 1000
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		fmt.Println(df)
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

// == model uid 2
func (f BaseFunction) GetChillerPlantCoolingLoad() error {
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
		fmt.Println("No data")
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 4 {
				return math.NaN()
			} else {
				return (f[0] + f[1] + f[2] + f[3])
			}

		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		fmt.Println(df)
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

// == model uid 3
func (f BaseFunction) GetChillerPlantCoP() error {
	newFunctionType := "Chiller_Plant_CoP"
	// newEquipmentName := "Chiller_Plant"
	newId := "Overall_Chiller_Plant_CoP"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Cooling_Load' OR
			"FunctionType"='Chiller_Plant_Total_Chiller_Energy') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	fmt.Println(dfGroup)
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
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
		fmt.Println(df)
	}
	wg.Wait()
	return nil
}

// == model uid 4
func (f BaseFunction) GetChillerPlantDeltaT() error {
	newFunctionType := "Chiller_Delta_T"
	newId := "Overall_Chiller_Plant_Delta_T"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Plant_Chilled_Water_Supply_Temperature_Sensor') AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	fmt.Println(dfGroup)
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
			lsss := client.WriteDfGroup(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
			err := client.InfluxdbWritePoints(lsss, "WIIOT")
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 1)
		wg.Wait()
		fmt.Println(df)
	}
	return nil
}

// == model uid 5
func (f BaseFunction) GetChillerPlantWetBulb() error {
	newFunctionType := "Chiller_Plant_Wet_Bulb"
	newId := "Chiller_Plant_Wet_Bulb"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Outdoor_Dry_Bulb' OR
			"FunctionType"='Chiller_Plant_Outdoor_Humidity') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
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
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
		fmt.Println(df)
	}
	return nil
}

// == model uid 6
func (f BaseFunction) GetChillerPlantCoP_kWPerTon() error {
	newFunctionType := "Chiller_Plant_CoP(kW/Ton)"
	newId := "Overall_Chiller_Plant_CoP(kW/Ton)"
	// newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Cooling_Load' OR
			"FunctionType"='Chiller_Plant_Total_Chiller_Energy') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
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
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
		fmt.Println(df)
	}
	return nil
}

// == model uid 7
func (f BaseFunction) GetChillerPlantCTRunning() error {
	newFunctionType := "Chiller_Plant_Total_Cooling_Tower_Running"
	newId := "Total_Cooling_Tower_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Cooling_Tower_Total_Status' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
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
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		fmt.Println(df)
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

// == model uid 8
func (f BaseFunction) GetChillerPlantPCHWPRunning() error {
	newId := "Total_PCHWP_Running"
	newFunctionType := "Chiller_Plant_Total_PCHWP_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Primary_Chilled_Water_Pump_Status' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	df, err := tool.ConcatDataframe(dfGroup)
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
		fmt.Println(df)
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

// == model uid 9
func (f BaseFunction) GetChillerPlantSCHWPRunning() error {
	newFunctionType := "Chiller_Plant_Total_SCHWP_Running"
	newId := "Total_SCHWP_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Secondary_Chilled_Water_Pump_Status_01' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	df, err := tool.ConcatDataframe(dfGroup)
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
		fmt.Println(df)
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

// == model uid 10
func (f BaseFunction) GetChillerPlantCTEnergy() error {
	newFunctionType := "Chiller_Plant_Total_CT_Energy"
	newId := "Total_CT_Energy"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Cooling_Tower_Total_Status' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		fmt.Println("No data")
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return (f[0] + f[1] + f[2] + f[3]) * 11
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		fmt.Println(df)
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

// == model uid 11
func (f BaseFunction) GetChillerPlantTotalEnergy() error {
	newFunctionType := "Chiller_Plant_Total_Energy"
	newId := "Total_Energy"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Total_CT_Energy' OR
			"FunctionType"='Chiller_Plant_Total_Chiller_Energy') AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	fmt.Println(dfGroup)
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
			lsss := client.WriteDfGroup(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
			err := client.InfluxdbWritePoints(lsss, "WIIOT")
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 1)
		fmt.Println(df)
	}
	wg.Wait()
	return nil
}

// == model uid 12
func (f BaseFunction) GetChillerPlantCoolingLoadTon() error {
	newFunctionType := "Total_Chiller_Plant_Cooling_Load(Ton)"
	newId := "Chiller_Plant_Cooling_Load_Ton"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Plant_Cooling_Load') AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	fmt.Println(dfGroup)
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
			lsss := client.WriteDfGroup(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, newId, df, 1)
			err := client.InfluxdbWritePoints(lsss, "WIIOT")
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 1)
		wg.Wait()
		fmt.Println(df)
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////

// individual model uid 0
func (f BaseFunction) GetChillerEnergy1Hour() error {
	newFunctionType := "Chiller_Energy"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-360m GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 1{
				return math.NaN()
			}
			return f[0] / 1000
		}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		fmt.Println(df)
		
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 1)
	}
	wg.Wait()
	return nil
}

// individual model uid 1
func (f BaseFunction) GetChillerEnergy1Day() error {
	newFunctionType := "Chiller_Energy"
	query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-4d GROUP BY EquipmentName, FunctionType, id, time(1d)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 1{
				return math.NaN()
			}
			return f[0] / 1000 / 3
		}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		fmt.Println(df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 1)
	}
	wg.Wait()
	return nil
}

// individual model uid 2
func (f BaseFunction) GetChillerEnergy1Month() error {
	newFunctionType := "Chiller_Energy"
	query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			( time>'%s' AND time<now() ) 
			GROUP BY EquipmentName, FunctionType, id`, f.Measurement, tool.GetCurrenttimeString())
	dfGroup := client.QueryDfGroup(query, f.Database)
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 1{
				return math.NaN()
			}
			return f[0] / 1000 / 3
		}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		fmt.Println(df)
		go func (query string, database string, measurement string,
				EquipmentName string, FunctionType string, id string,
				df dataframe.DataFrame, startIndex int)  {
			err := client.UploadDfGroup(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 1)
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 0)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	wg.Wait()
	return nil
}

// individual model uid 3
func (f BaseFunction) GetChillerCL() error {
	newFunctionType := "Chiller_Cooling_Load"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Water_Flowrate') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 3{
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
			err := client.UploadDfGroup(query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 1)
		fmt.Println(df)
	}
	wg.Wait()
	return nil
}

// individual model uid 4
func (f BaseFunction) GetChillerCoP() error {
	newFunctionType := "Chiller_CoP"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Power_Sensor' OR
			"FunctionType"='Chiller_Water_Flowrate') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 4{
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
			err := client.UploadDfGroup(query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 1)
		fmt.Println(df)
	}
	wg.Wait()
	return nil
}

// individual model uid 5
func (f BaseFunction) GetChillerDeltaT() error {
	newFunctionType := "Chiller_Delta_T"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor') AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 2{
				return math.NaN()
			}
			return f[0] - f[1]
		}, []int{1, 2}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		fmt.Println(df)
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 1)
	}
	wg.Wait()
	return nil
}

// individual model uid 6
func (f BaseFunction) GetChillerPlantEnergy1Hour() error {
	newFunctionType := "Chiller_Plant_Energy(60T)"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-240m GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	df, err := tool.ConcatDataframe(dfGroup)
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 4{
				return math.NaN()
			}
			return (f[0] + f[1] + f[2] + f[3]) / 1000
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newFunctionType, df, 1)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

// individual model uid 7
func (f BaseFunction) GetChillerPlantEnergy1Day() error {
	newFunctionType := "Chiller_Plant_Energy(1d)"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-4d GROUP BY EquipmentName, FunctionType, id, time(1d)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	df, err := tool.ConcatDataframe(dfGroup)
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 4{
				return math.NaN()
			}
			return (f[0] + f[1] + f[2] + f[3]) / 1000 / 3
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newFunctionType, df, 1)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

// individual model uid 8
func (f BaseFunction) GetChillerPlantEnergy1Month() error {
	newFunctionType := "Chiller_Plant_Energy(1d)"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT SUM(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			( time>'%s' AND time<now() ) 
			GROUP BY EquipmentName, FunctionType, id`, f.Measurement, tool.GetCurrenttimeString())
	dfGroup := client.QueryDfGroup(query, f.Database)
	df, err := tool.ConcatDataframe(dfGroup)
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 4{
				return math.NaN()
			}
			return (f[0] + f[1] + f[2] + f[3]) / 1000 / 3
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newFunctionType, df, 0)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

// individual model uid 9
func (f BaseFunction) GetChillerCoPkWPerTon() error {
	newFunctionType := "Chiller_CoP"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Power_Sensor' OR
			"FunctionType"='Chiller_Water_Flowrate') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 4{
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
			err := client.UploadDfGroup(query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 1)
		fmt.Println(df)
	}
	wg.Wait()
	return nil
}

// individual model uid 10
func (f BaseFunction) GetCTStatus() error {
	newFunctionType := "Chiller_CoP"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Cooling_Tower_Status_01' OR
			"FunctionType"='Cooling_Tower_Status_02' OR 
			"FunctionType"='Cooling_Tower_Status_03' OR
			"FunctionType"='Cooling_Tower_Status_04') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	wg := sync.WaitGroup{}
	wg.Add(len(dfGroup))
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if len(f) < 4{
				return math.NaN()
			} else if tool.ContainNaN(f) {
				return math.NaN()
			} else {
				return f[0]+f[1]+f[2]+f[3]
			}
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		go func(query string, database string, measurement string,
			EquipmentName string, FunctionType string, id string,
			df dataframe.DataFrame, startIndex int) {
			err := client.UploadDfGroup(query, database, measurement, EquipmentName, FunctionType, id, df, startIndex)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 1)
	}
	wg.Wait()
	return nil
}

