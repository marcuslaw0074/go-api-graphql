package functions

import (
	"fmt"
	"go-api-grapqhl/graph/client"
	logging "go-api-grapqhl/log"
	"go-api-grapqhl/tool"
)

var (
	Sands_Chiller = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
	Sands_CT      = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
	Sands_Logger  = logging.StartLogger("log/Sands_LogFile.log")
	update_Neo4j  = false
)

func (f BaseFunction) S_GetChillerPlantCTEnergy() error {
	url := fmt.Sprintf("http://%s:%v", f.Host, f.Port)
	name := "Sands_GetChillerPlantCTEnergy"
	Sands_Logger.Log(logging.LogInfo, "START function %s", name)
	newFunctionType := "Chiller_Plant_Total_CT_Energy"
	newId := "Total_CT_Energy"
	newEquipmentName := "Chiller_Plant"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='CT_Energy' AND 
			%s GROUP BY EquipmentName, FunctionType, id, time(60m)`, f.Measurement, TimeClause_Sands)
	dfGroup := client.QueryDfGroup(query, f.Database, f.Host, f.Port)
	fmt.Println(dfGroup)
	Sands_Logger.Log(logging.LogInfo, "function %s data: %v", name, dfGroup)
	df, err := tool.ConcatDataframe(dfGroup)
	if df.Nrow() == 0 {
		Sands_Logger.Log(logging.LogError, "function %s: No data", name)
		return nil
	} else if err != nil {
		return err
	} else {
		df = df.Rapply(tool.ApplyFunction(func(f ...float64) float64 {
			return tool.SumList(tool.GetNonNan(f))
		}, Sands_Chiller...)).Rename("Value", "X0").Mutate(df.Col("Time"))
		Sands_Logger.Log(logging.LogInfo, "function %s data: %v", name, df)
		err := client.UploadDfGroup(url, query, f.Database, f.Measurement, newEquipmentName, newFunctionType, newId, df, 1)
		if err != nil {
			return err
		}
		if update_Neo4j {
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
	}
	Sands_Logger.Log(logging.LogInfo, "END function %s", name)
	return nil
}
