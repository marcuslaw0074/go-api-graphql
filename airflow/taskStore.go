package airflow

func ComplexFunction() *Job {
	var k = BaseFunction{
		Database:    "WIIOT",
		Measurement: "Utility_3",
		Host:        "192.168.100.216",
		Port:        18086,
	}

	j := &Job{
		Name:     "test",
		Schedule: "* * * * *",
	}

	j.Add(&Task{
		BaseFunction: k,
		FunctionName: "GetChillerDeltaT",
		Name:         "haha",
	})

	j.Add(&Task{
		BaseFunction: k,
		FunctionName: "GetChillerPower",
		Name:         "haha2",
	})

	j.Add(&Task{
		BaseFunction: k,
		FunctionName: "GetChillerEnergy",
		Name:         "haha3",
	})

	j.Add(&Task{
		BaseFunction: k,
		FunctionName: "GetChillerPlantChillerRunning",
		Name:         "haha4",
	})

	j.SetDownstream(j.Task("haha"), j.Task("haha2"))
	j.SetDownstream(j.Task("haha3"), j.Task("haha2"))
	j.SetDownstream(j.Task("haha2"), j.Task("haha4"))
	j.run()
	return j
}