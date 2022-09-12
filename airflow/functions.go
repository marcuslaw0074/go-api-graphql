package airflow

import (
	"fmt"
	"go-api-grapqhl/graph/client"
	"go-api-grapqhl/tool"
	"math"
)

type BaseFunction struct {
	Host        string
	Port        int
	Database    string
	Measurement string
}

func (f BaseFunction) GetChillerPlantChillerRunning() error {
	newFunctionType := "Chiller_Plant_Total_Chiller_Running"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	df, err := tool.ConcatDataframe(dfGroup)
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
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
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newFunctionType, df, 0)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

func (f BaseFunction) GetChillerPlantEnergy() error {
	newFunctionType := "Chiller_Plant_Energy"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	df, err := tool.ConcatDataframe(dfGroup)
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return (f[0] + f[1] + f[2] + f[3]) / 1000
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, "Chiller_Plant_Total_Chiller_Energy", df, 0)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}



///////////////////////////////////////////////////////////////////////////////

func (f BaseFunction) GetChillerDeltaT() error {
	newFunctionType := "Chiller_Delta_T"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor') AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return f[0] - f[1]
		}, []int{1, 2}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 0)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

func (f BaseFunction) GetChillerCoP() error {
	newFunctionType := "Chiller_CoP"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Power_Sensor' OR
			"FunctionType"='Chiller_Water_Flowrate') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if f[2]/1000 > 50 && f[3] > 100 && (f[0]-f[1]) > 0 {
				return (f[0] - f[1]) * 4.2 * 0.0631 * f[3] / f[2] * 1000
			} else if tool.ContainNaN(f) {
				return math.NaN()
			} else {
				return 0
			}
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 0)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
		fmt.Println(df)
	}
	return nil
}

func (f BaseFunction) GetChillerCL() error {
	newFunctionType := "Chiller_Cooling_Load"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Water_Flowrate') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			if f[2] > 500 && (f[0]-f[1]) > 0 {
				return (f[0] - f[1]) * 4.2 * 0.0631 * f[2]
			} else if tool.ContainNaN(f) {
				return math.NaN()
			} else {
				return 0
			}
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 0)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
		fmt.Println(df)
	}
	return nil
}

func (f BaseFunction) GetChillerPower() error {
	newFunctionType := "Chiller_Delta_T"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	for _, ele := range dfGroup {
		df := tool.DiffValue(ele.Dataframe, 1, "Chiller_Power_Sensor").Rename("Value", "Chiller_Power_Sensor")
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 0)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

func (f BaseFunction) GetChillerEnergy() error {
	newFunctionType := "Chiller_Energy"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return f[0] / 1000
		}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, ele.EquipmentName, newFunctionType, "", df, 0)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

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
			return (f[0] + f[1] + f[2] + f[3]) / 1000
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newFunctionType, df, 0)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}

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



func (f BaseFunction) GetChillerPlantCoolingLoad() error {
	newFunctionType := "Chiller_Plant_Cooling_Load"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE ("FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' OR 
			"FunctionType"='Chiller_Water_Flowrate') AND
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, f.Measurement)
	dfGroup := client.QueryDfGroup(query, f.Database)
	dfGroup = client.ApplyFunctionDfGroup(dfGroup, func(f ...float64) float64 {
		if f[2] > 500 && (f[0]-f[1]) > 0 {
			return (f[0] - f[1]) * 4.2 * 0.0631 * f[2]
		} else if tool.ContainNaN(f) {
			return math.NaN()
		} else {
			return 0
		}
	}, "Chiller_Cooling_Load", []int{1, 2, 3}...)
	df, err := tool.ConcatDataframe(dfGroup)
	if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return (f[0] + f[1] + f[2] + f[3]) 
		}, []int{1, 2, 3, 4}...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		lsss := client.WriteDfGroup(query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newFunctionType, df, 0)
		err := client.InfluxdbWritePoints(lsss, "WIIOT")
		if err != nil {
			return err
		}
	}
	return nil
}
