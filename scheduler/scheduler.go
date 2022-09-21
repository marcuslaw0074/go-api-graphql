package scheduler

import (
	"fmt"
	"go-api-grapqhl/airflow"
	"go-api-grapqhl/functions"
)

func Analytics_Utility_3() *airflow.Job {

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
		Name:         fmt.Sprintf("%s_GetChillerPlantChillerRunning", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantChillerEnergy",
		Name:         fmt.Sprintf("%s_GetChillerPlantChillerEnergy", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoolingLoad",
		Name:         fmt.Sprintf("%s_GetChillerPlantCoolingLoad", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoP",
		Name:         fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantDeltaT",
		Name:         fmt.Sprintf("%s_GetChillerPlantDeltaT", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantWetBulb",
		Name:         fmt.Sprintf("%s_GetChillerPlantWetBulb", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoP_kWPerTon",
		Name:         fmt.Sprintf("%s_GetChillerPlantCoP_kWPerTon", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCTRunning",
		Name:         fmt.Sprintf("%s_GetChillerPlantCTRunning", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantPCHWPRunning",
		Name:         fmt.Sprintf("%s_GetChillerPlantPCHWPRunning", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantSCHWPRunning",
		Name:         fmt.Sprintf("%s_GetChillerPlantSCHWPRunning", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCTEnergy",
		Name:         fmt.Sprintf("%s_GetChillerPlantCTEnergy", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantTotalEnergy",
		Name:         fmt.Sprintf("%s_GetChillerPlantTotalEnergy", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoolingLoadTon",
		Name:         fmt.Sprintf("%s_GetChillerPlantCoolingLoadTon", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy1Hour",
		Name:         fmt.Sprintf("%s_GetChillerEnergy1Hour", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy1Day",
		Name:         fmt.Sprintf("%s_GetChillerEnergy1Day", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy1Month",
		Name:         fmt.Sprintf("%s_GetChillerEnergy1Month", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerDeltaT",
		Name:         fmt.Sprintf("%s_GetChillerDeltaT", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerCL",
		Name:         fmt.Sprintf("%s_GetChillerCL", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerCoP",
		Name:         fmt.Sprintf("%s_GetChillerCoP", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantEnergy1Hour",
		Name:         fmt.Sprintf("%s_GetChillerPlantEnergy1Hour", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantEnergy1Day",
		Name:         fmt.Sprintf("%s_GetChillerPlantEnergy1Day", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantEnergy1Month",
		Name:         fmt.Sprintf("%s_GetChillerPlantEnergy1Month", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerCoPkWPerTon",
		Name:         fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetCTStatus",
		Name:         fmt.Sprintf("%s_GetCTStatus", k.Measurement),
	})

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantChillerRunning", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCTRunning", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCTRunning", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantPCHWPRunning", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantPCHWPRunning", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantSCHWPRunning", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantChillerEnergy", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCTEnergy", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCTEnergy", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantTotalEnergy", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantDeltaT", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoolingLoad", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoolingLoad", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoolingLoadTon", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoolingLoadTon", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoP_kWPerTon", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantTotalEnergy", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerEnergy1Hour", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerDeltaT", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Hour", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerEnergy1Hour", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerEnergy1Day", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerEnergy1Day", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerEnergy1Month", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerEnergy1Month", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerDeltaT", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCL", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerCL", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoP", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Hour", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Day", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Day", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Month", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Month", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetCTStatus", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement)))

	j.Run()
	return j
}

func Analytics_Utility_2() *airflow.Job {

	k := functions.BaseFunction{
		Database:    "WIIOT",
		Measurement: "Utility_2",
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
		Name:         fmt.Sprintf("%s_GetChillerPlantChillerRunning", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantChillerEnergy",
		Name:         fmt.Sprintf("%s_GetChillerPlantChillerEnergy", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoolingLoad",
		Name:         fmt.Sprintf("%s_GetChillerPlantCoolingLoad", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoP",
		Name:         fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantDeltaT",
		Name:         fmt.Sprintf("%s_GetChillerPlantDeltaT", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantWetBulb",
		Name:         fmt.Sprintf("%s_GetChillerPlantWetBulb", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoP_kWPerTon",
		Name:         fmt.Sprintf("%s_GetChillerPlantCoP_kWPerTon", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCTRunning",
		Name:         fmt.Sprintf("%s_GetChillerPlantCTRunning", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantPCHWPRunning",
		Name:         fmt.Sprintf("%s_GetChillerPlantPCHWPRunning", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantSCHWPRunning",
		Name:         fmt.Sprintf("%s_GetChillerPlantSCHWPRunning", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCTEnergy",
		Name:         fmt.Sprintf("%s_GetChillerPlantCTEnergy", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantTotalEnergy",
		Name:         fmt.Sprintf("%s_GetChillerPlantTotalEnergy", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoolingLoadTon",
		Name:         fmt.Sprintf("%s_GetChillerPlantCoolingLoadTon", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy1Hour",
		Name:         fmt.Sprintf("%s_GetChillerEnergy1Hour", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy1Day",
		Name:         fmt.Sprintf("%s_GetChillerEnergy1Day", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy1Month",
		Name:         fmt.Sprintf("%s_GetChillerEnergy1Month", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerDeltaT",
		Name:         fmt.Sprintf("%s_GetChillerDeltaT", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerCL",
		Name:         fmt.Sprintf("%s_GetChillerCL", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerCoP",
		Name:         fmt.Sprintf("%s_GetChillerCoP", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantEnergy1Hour",
		Name:         fmt.Sprintf("%s_GetChillerPlantEnergy1Hour", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantEnergy1Day",
		Name:         fmt.Sprintf("%s_GetChillerPlantEnergy1Day", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantEnergy1Month",
		Name:         fmt.Sprintf("%s_GetChillerPlantEnergy1Month", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerCoPkWPerTon",
		Name:         fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetCTStatus",
		Name:         fmt.Sprintf("%s_GetCTStatus", k.Measurement),
	})

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantChillerRunning", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCTRunning", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCTRunning", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantPCHWPRunning", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantPCHWPRunning", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantSCHWPRunning", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantChillerEnergy", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCTEnergy", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCTEnergy", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantTotalEnergy", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantDeltaT", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoolingLoad", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoolingLoad", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoolingLoadTon", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoolingLoadTon", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoP_kWPerTon", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantTotalEnergy", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerEnergy1Hour", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerDeltaT", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Hour", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerEnergy1Hour", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerEnergy1Day", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerEnergy1Day", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerEnergy1Month", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerEnergy1Month", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerDeltaT", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCL", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerCL", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoP", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Hour", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Day", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Day", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Month", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Month", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetCTStatus", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement)))

	j.Run()
	return j
}

func Analytics_Utility_1() *airflow.Job {

	k := functions.BaseFunction{
		Database:    "WIIOT",
		Measurement: "Utility_1",
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
		Name:         fmt.Sprintf("%s_GetChillerPlantChillerRunning", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantChillerEnergy",
		Name:         fmt.Sprintf("%s_GetChillerPlantChillerEnergy", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoolingLoad",
		Name:         fmt.Sprintf("%s_GetChillerPlantCoolingLoad", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoP",
		Name:         fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantDeltaT",
		Name:         fmt.Sprintf("%s_GetChillerPlantDeltaT", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantWetBulb",
		Name:         fmt.Sprintf("%s_GetChillerPlantWetBulb", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoP_kWPerTon",
		Name:         fmt.Sprintf("%s_GetChillerPlantCoP_kWPerTon", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCTRunning",
		Name:         fmt.Sprintf("%s_GetChillerPlantCTRunning", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantPCHWPRunning",
		Name:         fmt.Sprintf("%s_GetChillerPlantPCHWPRunning", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantSCHWPRunning",
		Name:         fmt.Sprintf("%s_GetChillerPlantSCHWPRunning", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCTEnergy",
		Name:         fmt.Sprintf("%s_GetChillerPlantCTEnergy", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantTotalEnergy",
		Name:         fmt.Sprintf("%s_GetChillerPlantTotalEnergy", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantCoolingLoadTon",
		Name:         fmt.Sprintf("%s_GetChillerPlantCoolingLoadTon", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy1Hour",
		Name:         fmt.Sprintf("%s_GetChillerEnergy1Hour", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy1Day",
		Name:         fmt.Sprintf("%s_GetChillerEnergy1Day", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy1Month",
		Name:         fmt.Sprintf("%s_GetChillerEnergy1Month", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerDeltaT",
		Name:         fmt.Sprintf("%s_GetChillerDeltaT", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerCL",
		Name:         fmt.Sprintf("%s_GetChillerCL", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerCoP",
		Name:         fmt.Sprintf("%s_GetChillerCoP", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantEnergy1Hour",
		Name:         fmt.Sprintf("%s_GetChillerPlantEnergy1Hour", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantEnergy1Day",
		Name:         fmt.Sprintf("%s_GetChillerPlantEnergy1Day", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantEnergy1Month",
		Name:         fmt.Sprintf("%s_GetChillerPlantEnergy1Month", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerCoPkWPerTon",
		Name:         fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement),
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetCTStatus",
		Name:         fmt.Sprintf("%s_GetCTStatus", k.Measurement),
	})

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantChillerRunning", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCTRunning", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCTRunning", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantPCHWPRunning", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantPCHWPRunning", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantSCHWPRunning", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantChillerEnergy", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCTEnergy", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCTEnergy", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantTotalEnergy", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantDeltaT", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoolingLoad", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoolingLoad", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoolingLoadTon", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoolingLoadTon", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoP_kWPerTon", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantTotalEnergy", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerEnergy1Hour", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerDeltaT", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Hour", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerEnergy1Hour", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerEnergy1Day", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerEnergy1Day", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerEnergy1Month", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerEnergy1Month", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerDeltaT", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCL", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerCL", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoP", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerCoP", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Hour", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Day", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Day", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Month", k.Measurement)))
	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetChillerPlantEnergy1Month", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement)))

	j.SetDownstream(j.Task(fmt.Sprintf("%s_GetCTStatus", k.Measurement)), j.Task(fmt.Sprintf("%s_GetChillerCoPkWPerTon", k.Measurement)))

	j.Run()
	return j
}

func Test_Analytics() *airflow.Job {

	k := functions.BaseFunction{
		Database:    "WIIOT",
		Measurement: "Utility_1",
		Host:        "192.168.100.216",
		Port:        18086,
	}

	j := &airflow.Job{
		Name:     "test",
		Schedule: "* * * * *",
	}

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility1_GetChillerCoP",
		Name:         fmt.Sprintf("%s_GetChillerEnergy", k.Measurement),
	})

	j.Run()
	return j
}