package scheduler

import (
	"go-api-grapqhl/airflow"
	"go-api-grapqhl/functions"
)

func Analytics() *airflow.Job {

	k := functions.BaseFunction{
		Database:    "WIIOT",
		Measurement: "Utility_3",
		Host:        "192.168.100.216",
		Port:        18086,
	}

	j := &airflow.Job{
		Name:     "test",
		Schedule: "* * * * *",
	}

	// j.Add(&airflow.Task{
	// 	BaseFunction: k,
	// 	FunctionName: "GetChillerPlantChillerRunning",
	// 	Name:         "GetChillerPlantChillerRunning",
	// })

	// j.Add(&airflow.Task{
	// 	BaseFunction: k,
	// 	FunctionName: "GetChillerPlantChillerEnergy",
	// 	Name:         "GetChillerPlantChillerEnergy",
	// })

	// j.Add(&airflow.Task{
	// 	BaseFunction: k,
	// 	FunctionName: "GetChillerPlantCoolingLoad",
	// 	Name:         "GetChillerPlantCoolingLoad",
	// })

	// j.Add(&airflow.Task{
	// 	BaseFunction: k,
	// 	FunctionName: "GetChillerPlantCoP",
	// 	Name:         "GetChillerPlantCoP",
	// })

	// j.Add(&airflow.Task{
	// 	BaseFunction: k,
	// 	FunctionName: "GetChillerPlantDeltaT",
	// 	Name:         "GetChillerPlantDeltaT",
	// })

	// j.Add(&airflow.Task{
	// 	BaseFunction: k,
	// 	FunctionName: "GetChillerPlantWetBulb",
	// 	Name:         "GetChillerPlantWetBulb",
	// })

	// j.Add(&airflow.Task{
	// 	BaseFunction: k,
	// 	FunctionName: "GetChillerPlantCoP_kWPerTon",
	// 	Name:         "GetChillerPlantCoP_kWPerTon",
	// })

	// j.Add(&airflow.Task{
	// 	BaseFunction: k,
	// 	FunctionName: "GetChillerPlantCTRunning",
	// 	Name:         "GetChillerPlantCTRunning",
	// })

	// j.Add(&airflow.Task{
	// 	BaseFunction: k,
	// 	FunctionName: "GetChillerPlantPCHWPRunning",
	// 	Name:         "GetChillerPlantPCHWPRunning",
	// })

	// j.Add(&airflow.Task{
	// 	BaseFunction: k,
	// 	FunctionName: "GetChillerPlantSCHWPRunning",
	// 	Name:         "GetChillerPlantSCHWPRunning",
	// })

	// j.Add(&airflow.Task{
	// 	BaseFunction: k,
	// 	FunctionName: "GetChillerPlantCTEnergy",
	// 	Name:         "GetChillerPlantCTEnergy",
	// })

	// j.Add(&airflow.Task{
	// 	BaseFunction: k,
	// 	FunctionName: "GetChillerPlantTotalEnergy",
	// 	Name:         "GetChillerPlantTotalEnergy",
	// })

	// j.Add(&airflow.Task{
	// 	BaseFunction: k,
	// 	FunctionName: "GetChillerPlantCoolingLoadTon",
	// 	Name:         "GetChillerPlantCoolingLoadTon",
	// })

	// j.SetDownstream(j.Task("GetChillerPlantChillerRunning"), j.Task("GetChillerPlantCTRunning"))
	// j.SetDownstream(j.Task("GetChillerPlantCTRunning"), j.Task("GetChillerPlantPCHWPRunning"))
	// j.SetDownstream(j.Task("GetChillerPlantPCHWPRunning"), j.Task("GetChillerPlantSCHWPRunning"))

	// j.SetDownstream(j.Task("GetChillerPlantChillerEnergy"), j.Task("GetChillerPlantCTEnergy"))
	// j.SetDownstream(j.Task("GetChillerPlantCTEnergy"), j.Task("GetChillerPlantTotalEnergy"))

	// j.SetDownstream(j.Task("GetChillerPlantDeltaT"), j.Task("GetChillerPlantCoolingLoad"))
	// j.SetDownstream(j.Task("GetChillerPlantCoolingLoad"), j.Task("GetChillerPlantCoolingLoadTon"))
	// j.SetDownstream(j.Task("GetChillerPlantCoolingLoadTon"), j.Task("GetChillerPlantCoP"))
	// j.SetDownstream(j.Task("GetChillerPlantCoP"), j.Task("GetChillerPlantCoP_kWPerTon"))

	// j.SetDownstream(j.Task("GetChillerPlantTotalEnergy"), j.Task("GetChillerPlantCoP"))

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoP",
		Name:         "GetChillerPlantCoP",
	})

	j.Run()
	return j
}
