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

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantChillerRunning",
		Name:         "GetChillerPlantChillerRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantChillerEnergy",
		Name:         "GetChillerPlantChillerEnergy",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoolingLoad",
		Name:         "GetChillerPlantCoolingLoad",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoP",
		Name:         "GetChillerPlantCoP",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantDeltaT",
		Name:         "GetChillerPlantDeltaT",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantWetBulb",
		Name:         "GetChillerPlantWetBulb",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoP_kWPerTon",
		Name:         "GetChillerPlantCoP_kWPerTon",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCTRunning",
		Name:         "GetChillerPlantCTRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantPCHWPRunning",
		Name:         "GetChillerPlantPCHWPRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantSCHWPRunning",
		Name:         "GetChillerPlantSCHWPRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCTEnergy",
		Name:         "GetChillerPlantCTEnergy",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantTotalEnergy",
		Name:         "GetChillerPlantTotalEnergy",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoolingLoadTon",
		Name:         "GetChillerPlantCoolingLoadTon",
	})

	j.SetDownstream(j.Task("GetChillerPlantChillerRunning"), j.Task("GetChillerPlantCTRunning"))
	j.SetDownstream(j.Task("GetChillerPlantCTRunning"), j.Task("GetChillerPlantPCHWPRunning"))
	j.SetDownstream(j.Task("GetChillerPlantPCHWPRunning"), j.Task("GetChillerPlantSCHWPRunning"))

	j.SetDownstream(j.Task("GetChillerPlantChillerEnergy"), j.Task("GetChillerPlantCTEnergy"))
	j.SetDownstream(j.Task("GetChillerPlantCTEnergy"), j.Task("GetChillerPlantTotalEnergy"))

	j.SetDownstream(j.Task("GetChillerPlantDeltaT"), j.Task("GetChillerPlantCoolingLoad"))
	j.SetDownstream(j.Task("GetChillerPlantCoolingLoad"), j.Task("GetChillerPlantCoolingLoadTon"))
	j.SetDownstream(j.Task("GetChillerPlantCoolingLoadTon"), j.Task("GetChillerPlantCoP"))
	j.SetDownstream(j.Task("GetChillerPlantCoP"), j.Task("GetChillerPlantCoP_kWPerTon"))

	j.SetDownstream(j.Task("GetChillerPlantTotalEnergy"), j.Task("GetChillerPlantCoP"))

	j.Run()
	return j
}

func IndividualAnalytics() *airflow.Job {

	k := functions.BaseFunction{
		Database:    "WIIOT",
		Measurement: "Utility_3",
		Host:        "192.168.100.216",
		Port:        18086,
	}

	j := &airflow.Job{
		Name:     "test2",
		Schedule: "* * * * *",
	}

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy1Hour",
		Name:         "GetChillerEnergy1Hour",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy1Day",
		Name:         "GetChillerEnergy1Day",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy1Month",
		Name:         "GetChillerEnergy1Month",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerDeltaT",
		Name:         "GetChillerDeltaT",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerCL",
		Name:         "GetChillerCL",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerCoP",
		Name:         "GetChillerCoP",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantEnergy1Hour",
		Name:         "GetChillerPlantEnergy1Hour",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantEnergy1Day",
		Name:         "GetChillerPlantEnergy1Day",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantEnergy1Month",
		Name:         "GetChillerPlantEnergy1Month",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerCoPkWPerTon",
		Name:         "GetChillerCoPkWPerTon",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetCTStatus",
		Name:         "GetCTStatus",
	})

	j.SetDownstream(j.Task("GetChillerEnergy1Hour"), j.Task("GetChillerEnergy1Day"))
	j.SetDownstream(j.Task("GetChillerEnergy1Day"), j.Task("GetChillerEnergy1Month"))
	j.SetDownstream(j.Task("GetChillerEnergy1Month"), j.Task("GetChillerCoPkWPerTon"))

	j.SetDownstream(j.Task("GetChillerDeltaT"), j.Task("GetChillerCL"))
	j.SetDownstream(j.Task("GetChillerCL"), j.Task("GetChillerCoP"))
	j.SetDownstream(j.Task("GetChillerCoP"), j.Task("GetChillerCoPkWPerTon"))

	j.SetDownstream(j.Task("GetChillerPlantEnergy1Hour"), j.Task("GetChillerPlantEnergy1Day"))
	j.SetDownstream(j.Task("GetChillerPlantEnergy1Day"), j.Task("GetChillerPlantEnergy1Month"))
	j.SetDownstream(j.Task("GetChillerPlantEnergy1Month"), j.Task("GetChillerCoPkWPerTon"))

	j.SetDownstream(j.Task("GetCTStatus"), j.Task("GetChillerCoPkWPerTon"))

	j.Run()
	return j
}

