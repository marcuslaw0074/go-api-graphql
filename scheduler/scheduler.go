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
		FunctionName: "GetChillerDeltaT",
		Name:         "haha",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPower",
		Name:         "haha2",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy",
		Name:         "haha3",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantWetBulb",
		Name:         "haha4",
	})

	j.SetDownstream(j.Task("haha"), j.Task("haha2"))
	j.SetDownstream(j.Task("haha3"), j.Task("haha2"))
	j.SetDownstream(j.Task("haha2"), j.Task("haha4"))
	j.Run()
	return j
}