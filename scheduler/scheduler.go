package scheduler

import (
	"fmt"
	"go-api-grapqhl/airflow"
	"go-api-grapqhl/functions"
	"os"
	"strconv"
)

func Analytics_Utility_3() *airflow.Job {

	port, err := strconv.Atoi(os.Getenv("INFLUX_PORT"))
	if err != nil {
		fmt.Println("Port Error")
	}

	neo4jport, err2 := strconv.Atoi(os.Getenv("NEO4J_PORT"))
	if err2 != nil {
		fmt.Println("Port Error")
	}

	k := functions.BaseFunction{
		Database:       os.Getenv("INFLUX_DATABASE"),
		Measurement:    os.Getenv("INFLUX_MEASUREMENT_3"),
		Host:           os.Getenv("INFLUX_HOST"),
		Port:           port,
		Neo4j_Host:     os.Getenv("NEO4J_HOST"),
		Neo4j_Port:     neo4jport,
		Neo4j_Database: os.Getenv("NEO4J_DATABASE"),
		Neo4j_Username: os.Getenv("NEO4J_USERNAME"),
		Neo4j_Password: os.Getenv("NEO4J_PASSWORD"),
	}

	j := &airflow.Job{
		Name:     "test",
		Schedule: "* * * * *",
	}

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantChillerRunning",
		Name:         "Utility3_GetChillerPlantChillerRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantChillerEnergy",
		Name:         "Utility3_GetChillerPlantChillerEnergy",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantCoolingLoad",
		Name:         "Utility3_GetChillerPlantCoolingLoad",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantCoP",
		Name:         "Utility3_GetChillerPlantCoP",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantDeltaT",
		Name:         "Utility3_GetChillerPlantDeltaT",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantWetBulb",
		Name:         "Utility3_GetChillerPlantWetBulb",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantCoP_kWPerTon",
		Name:         "Utility3_GetChillerPlantCoP_kWPerTon",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantCTRunning",
		Name:         "Utility3_GetChillerPlantCTRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantPCHWPRunning",
		Name:         "Utility3_GetChillerPlantPCHWPRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantSCHWPRunning",
		Name:         "Utility3_GetChillerPlantSCHWPRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantCTEnergy",
		Name:         "Utility3_GetChillerPlantCTEnergy",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantTotalEnergy",
		Name:         "Utility3_GetChillerPlantTotalEnergy",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantCoolingLoadTon",
		Name:         "Utility3_GetChillerPlantCoolingLoadTon",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerEnergy1Hour",
		Name:         "Utility3_GetChillerEnergy1Hour",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerEnergy1Day",
		Name:         "Utility3_GetChillerEnergy1Day",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerEnergy1Month",
		Name:         "Utility3_GetChillerEnergy1Month",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerDeltaT",
		Name:         "Utility3_GetChillerDeltaT",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerCL",
		Name:         "Utility3_GetChillerCL",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerCoP",
		Name:         "Utility3_GetChillerCoP",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantEnergy1Hour",
		Name:         "Utility3_GetChillerPlantEnergy1Hour",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantEnergy1Day",
		Name:         "Utility3_GetChillerPlantEnergy1Day",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerPlantEnergy1Month",
		Name:         "Utility3_GetChillerPlantEnergy1Month",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerCoPkWPerTon",
		Name:         "Utility3_GetChillerCoPkWPerTon",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetCTStatus",
		Name:         "Utility3_GetCTStatus",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility3_GetChillerCLTon",
		Name:         "Utility3_GetChillerCLTon",
	})

	// calculate total running status of each equipment type
	j.SetDownstream(j.Task("Utility3_GetChillerPlantChillerRunning"), j.Task("Utility3_GetChillerPlantCTRunning"))
	j.SetDownstream(j.Task("Utility3_GetChillerPlantCTRunning"), j.Task("Utility3_GetChillerPlantPCHWPRunning"))
	j.SetDownstream(j.Task("Utility3_GetChillerPlantPCHWPRunning"), j.Task("Utility3_GetChillerPlantSCHWPRunning"))

	// calculate total energy of each equipment type
	j.SetDownstream(j.Task("Utility3_GetChillerPlantChillerEnergy"), j.Task("Utility3_GetChillerPlantCTEnergy"))
	j.SetDownstream(j.Task("Utility3_GetChillerPlantCTEnergy"), j.Task("Utility3_GetChillerPlantTotalEnergy"))

	// calculate delta T, CL, CoP of whole chiller plant
	j.SetDownstream(j.Task("Utility3_GetChillerPlantDeltaT"), j.Task("Utility3_GetChillerPlantCoolingLoad"))
	j.SetDownstream(j.Task("Utility3_GetChillerPlantCoolingLoad"), j.Task("Utility3_GetChillerPlantCoolingLoadTon"))
	j.SetDownstream(j.Task("Utility3_GetChillerPlantCoolingLoadTon"), j.Task("Utility3_GetChillerPlantCoP"))
	j.SetDownstream(j.Task("Utility3_GetChillerPlantCoP"), j.Task("Utility3_GetChillerPlantCoP_kWPerTon"))
	j.SetDownstream(j.Task("Utility3_GetChillerPlantTotalEnergy"), j.Task("Utility3_GetChillerPlantCoP"))

	// set children node
	j.SetDownstream(j.Task("Utility3_GetChillerPlantCoP"), j.Task("Utility3_GetChillerEnergy1Hour"))
	j.SetDownstream(j.Task("Utility3_GetChillerPlantCoP"), j.Task("Utility3_GetChillerDeltaT"))

	// calculate hourly, daily, monthly energy consumption of individual chiller
	j.SetDownstream(j.Task("Utility3_GetChillerEnergy1Hour"), j.Task("Utility3_GetChillerEnergy1Day"))
	j.SetDownstream(j.Task("Utility3_GetChillerEnergy1Day"), j.Task("Utility3_GetChillerEnergy1Month"))
	j.SetDownstream(j.Task("Utility3_GetChillerEnergy1Month"), j.Task("Utility3_GetChillerPlantEnergy1Hour"))

	// calculate delta T, CL, CoP of individual chiller
	j.SetDownstream(j.Task("Utility3_GetChillerDeltaT"), j.Task("Utility3_GetChillerCL"))
	j.SetDownstream(j.Task("Utility3_GetChillerCL"), j.Task("Utility3_GetChillerCoP"))
	j.SetDownstream(j.Task("Utility3_GetChillerCoP"), j.Task("Utility3_GetChillerCLTon"))
	j.SetDownstream(j.Task("Utility3_GetChillerCLTon"), j.Task("Utility3_GetChillerCoPkWPerTon"))

	// calculate hourly, daily, monthly energy consumption of whole chiller plant
	j.SetDownstream(j.Task("Utility3_GetChillerPlantEnergy1Hour"), j.Task("Utility3_GetChillerPlantEnergy1Day"))
	j.SetDownstream(j.Task("Utility3_GetChillerPlantEnergy1Day"), j.Task("Utility3_GetChillerPlantEnergy1Month"))
	j.SetDownstream(j.Task("Utility3_GetChillerPlantEnergy1Month"), j.Task("Utility3_GetChillerCoPkWPerTon"))

	// calculate status of individual cooling tower
	j.SetDownstream(j.Task("Utility3_GetCTStatus"), j.Task("Utility3_GetChillerCoPkWPerTon"))

	j.Run()
	return j
}

func Analytics_Utility_2() *airflow.Job {

	port, err := strconv.Atoi(os.Getenv("INFLUX_PORT"))
	if err != nil {
		fmt.Println("Port Error")
	}
	neo4jport, err2 := strconv.Atoi(os.Getenv("NEO4J_PORT"))
	if err2 != nil {
		fmt.Println("Port Error")
	}
	k := functions.BaseFunction{
		Database:       os.Getenv("INFLUX_DATABASE"),
		Measurement:    os.Getenv("INFLUX_MEASUREMENT_2"),
		Host:           os.Getenv("INFLUX_HOST"),
		Port:           port,
		Neo4j_Host:     os.Getenv("NEO4J_HOST"),
		Neo4j_Port:     neo4jport,
		Neo4j_Database: os.Getenv("NEO4J_DATABASE"),
		Neo4j_Username: os.Getenv("NEO4J_USERNAME"),
		Neo4j_Password: os.Getenv("NEO4J_PASSWORD"),
	}

	j := &airflow.Job{
		Name:     "test",
		Schedule: "* * * * *",
	}

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantChillerRunning",
		Name:         "Utility2_GetChillerPlantChillerRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantChillerEnergy",
		Name:         "Utility2_GetChillerPlantChillerEnergy",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantCoolingLoad",
		Name:         "Utility2_GetChillerPlantCoolingLoad",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantCoP",
		Name:         "Utility2_GetChillerPlantCoP",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantDeltaT",
		Name:         "Utility2_GetChillerPlantDeltaT",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantWetBulb",
		Name:         "Utility2_GetChillerPlantWetBulb",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantCoP_kWPerTon",
		Name:         "Utility2_GetChillerPlantCoP_kWPerTon",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantCTRunning",
		Name:         "Utility2_GetChillerPlantCTRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantPCHWPRunning",
		Name:         "Utility2_GetChillerPlantPCHWPRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantSCHWPRunning",
		Name:         "Utility2_GetChillerPlantSCHWPRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantCTEnergy",
		Name:         "Utility2_GetChillerPlantCTEnergy",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantTotalEnergy",
		Name:         "Utility2_GetChillerPlantTotalEnergy",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantCoolingLoadTon",
		Name:         "Utility2_GetChillerPlantCoolingLoadTon",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerEnergy1Hour",
		Name:         "Utility2_GetChillerEnergy1Hour",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerEnergy1Day",
		Name:         "Utility2_GetChillerEnergy1Day",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerEnergy1Month",
		Name:         "Utility2_GetChillerEnergy1Month",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerDeltaT",
		Name:         "Utility2_GetChillerDeltaT",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerCL",
		Name:         "Utility2_GetChillerCL",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerCoP",
		Name:         "Utility2_GetChillerCoP",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantEnergy1Hour",
		Name:         "Utility2_GetChillerPlantEnergy1Hour",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantEnergy1Day",
		Name:         "Utility2_GetChillerPlantEnergy1Day",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerPlantEnergy1Month",
		Name:         "Utility2_GetChillerPlantEnergy1Month",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerCoPkWPerTon",
		Name:         "Utility2_GetChillerCoPkWPerTon",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetCTStatus",
		Name:         "Utility2_GetCTStatus",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility2_GetChillerCLTon",
		Name:         "Utility2_GetChillerCLTon",
	})

	// calculate total running status of each equipment type
	j.SetDownstream(j.Task("Utility2_GetChillerPlantChillerRunning"), j.Task("Utility2_GetChillerPlantCTRunning"))
	j.SetDownstream(j.Task("Utility2_GetChillerPlantCTRunning"), j.Task("Utility2_GetChillerPlantPCHWPRunning"))
	j.SetDownstream(j.Task("Utility2_GetChillerPlantPCHWPRunning"), j.Task("Utility2_GetChillerPlantSCHWPRunning"))

	// calculate total energy of each equipment type
	j.SetDownstream(j.Task("Utility2_GetChillerPlantChillerEnergy"), j.Task("Utility2_GetChillerPlantCTEnergy"))
	j.SetDownstream(j.Task("Utility2_GetChillerPlantCTEnergy"), j.Task("Utility2_GetChillerPlantTotalEnergy"))

	// calculate delta T, CL, CoP of whole chiller plant
	j.SetDownstream(j.Task("Utility2_GetChillerPlantDeltaT"), j.Task("Utility2_GetChillerPlantCoolingLoad"))
	j.SetDownstream(j.Task("Utility2_GetChillerPlantCoolingLoad"), j.Task("Utility2_GetChillerPlantCoolingLoadTon"))
	j.SetDownstream(j.Task("Utility2_GetChillerPlantCoolingLoadTon"), j.Task("Utility2_GetChillerPlantCoP"))
	j.SetDownstream(j.Task("Utility2_GetChillerPlantCoP"), j.Task("Utility2_GetChillerPlantCoP_kWPerTon"))
	j.SetDownstream(j.Task("Utility2_GetChillerPlantTotalEnergy"), j.Task("Utility2_GetChillerPlantCoP"))

	// set children node
	j.SetDownstream(j.Task("Utility2_GetChillerPlantCoP"), j.Task("Utility2_GetChillerEnergy1Hour"))
	j.SetDownstream(j.Task("Utility2_GetChillerPlantCoP"), j.Task("Utility2_GetChillerDeltaT"))

	// calculate hourly, daily, monthly energy consumption of individual chiller
	j.SetDownstream(j.Task("Utility2_GetChillerEnergy1Hour"), j.Task("Utility2_GetChillerEnergy1Day"))
	j.SetDownstream(j.Task("Utility2_GetChillerEnergy1Day"), j.Task("Utility2_GetChillerEnergy1Month"))

	j.SetDownstream(j.Task("Utility2_GetChillerPlantChillerEnergy"), j.Task("Utility2_GetChillerPlantEnergy1Hour"))

	// calculate delta T, CL, CoP of individual chiller
	j.SetDownstream(j.Task("Utility2_GetChillerDeltaT"), j.Task("Utility2_GetChillerCL"))
	j.SetDownstream(j.Task("Utility2_GetChillerCL"), j.Task("Utility2_GetChillerCoP"))
	j.SetDownstream(j.Task("Utility2_GetChillerCoP"), j.Task("Utility2_GetChillerCLTon"))
	j.SetDownstream(j.Task("Utility2_GetChillerCLTon"), j.Task("Utility2_GetChillerCoPkWPerTon"))

	// calculate hourly, daily, monthly energy consumption of whole chiller plant
	j.SetDownstream(j.Task("Utility2_GetChillerPlantEnergy1Hour"), j.Task("Utility2_GetChillerPlantEnergy1Day"))
	j.SetDownstream(j.Task("Utility2_GetChillerPlantEnergy1Day"), j.Task("Utility2_GetChillerPlantEnergy1Month"))
	j.SetDownstream(j.Task("Utility2_GetChillerPlantEnergy1Month"), j.Task("Utility2_GetChillerCoPkWPerTon"))

	// calculate status of individual cooling tower
	j.SetDownstream(j.Task("Utility2_GetCTStatus"), j.Task("Utility2_GetChillerCoPkWPerTon"))

	j.Run()
	return j
}

func Analytics_Utility_1() *airflow.Job {

	port, err := strconv.Atoi(os.Getenv("INFLUX_PORT"))
	if err != nil {
		fmt.Println("Port Error")
	}
	neo4jport, err2 := strconv.Atoi(os.Getenv("NEO4J_PORT"))
	if err2 != nil {
		fmt.Println("Port Error")
	}
	k := functions.BaseFunction{
		Database:       os.Getenv("INFLUX_DATABASE"),
		Measurement:    os.Getenv("INFLUX_MEASUREMENT_1"),
		Host:           os.Getenv("INFLUX_HOST"),
		Port:           port,
		Neo4j_Host:     os.Getenv("NEO4J_HOST"),
		Neo4j_Port:     neo4jport,
		Neo4j_Database: os.Getenv("NEO4J_DATABASE"),
		Neo4j_Username: os.Getenv("NEO4J_USERNAME"),
		Neo4j_Password: os.Getenv("NEO4J_PASSWORD"),
	}

	j := &airflow.Job{
		Name:     "test",
		Schedule: "* * * * *",
	}

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility1_GetChillerPlantChillerRunning",
		Name:         "Utility1_GetChillerPlantChillerRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility1_GetChillerPlantCoolingLoad",
		Name:         "Utility1_GetChillerPlantCoolingLoad",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility1_GetChillerPlantWetBulb",
		Name:         "Utility1_GetChillerPlantWetBulb",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility1_GetChillerEnergy1Hour",
		Name:         "Utility1_GetChillerEnergy1Hour",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility1_GetChillerEnergy1Day",
		Name:         "Utility1_GetChillerEnergy1Day",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility1_GetChillerEnergy1Month",
		Name:         "Utility1_GetChillerEnergy1Month",
	})
	
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility1_GetChillerCL",
		Name:         "Utility1_GetChillerCL",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility1_GetChillerDeltaT",
		Name:         "Utility1_GetChillerDeltaT",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Utility1_GetChillerCoP",
		Name:         "Utility1_GetChillerCoP",
	})


	// calculate delta T, CL, CoP of whole chiller plant
	j.SetDownstream(j.Task("Utility1_GetChillerPlantChillerRunning"), j.Task("Utility1_GetChillerPlantCoolingLoad"))

	// calculate delta T, CL, CoP of individual chiller
	j.SetDownstream(j.Task("Utility1_GetChillerDeltaT"), j.Task("Utility1_GetChillerCL"))
	j.SetDownstream(j.Task("Utility1_GetChillerCL"), j.Task("Utility1_GetChillerCoP"))

	// calculate hourly, daily, monthly energy consumption of individual chiller
	j.SetDownstream(j.Task("Utility1_GetChillerEnergy1Hour"), j.Task("Utility1_GetChillerEnergy1Day"))
	j.SetDownstream(j.Task("Utility1_GetChillerEnergy1Day"), j.Task("Utility1_GetChillerEnergy1Month"))

	j.Run()
	return j
}

func Analytics_HCity1() *airflow.Job {

	port, err := strconv.Atoi(os.Getenv("INFLUX_PORT"))
	if err != nil {
		fmt.Println("Port Error")
	}
	neo4jport, err2 := strconv.Atoi(os.Getenv("NEO4J_PORT"))
	if err2 != nil {
		fmt.Println("Port Error")
	}
	k := functions.BaseFunction{
		Database:       os.Getenv("INFLUX_DATABASE"),
		Measurement:    os.Getenv("INFLUX_MEASUREMENT_1"),
		Host:           os.Getenv("INFLUX_HOST"),
		Port:           port,
		Neo4j_Host:     os.Getenv("NEO4J_HOST"),
		Neo4j_Port:     neo4jport,
		Neo4j_Database: os.Getenv("NEO4J_DATABASE"),
		Neo4j_Username: os.Getenv("NEO4J_USERNAME"),
		Neo4j_Password: os.Getenv("NEO4J_PASSWORD"),
	}

	j := &airflow.Job{
		Name:     "test",
		Schedule: "* * * * *",
	}

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "HCity1_GetChillerPlantChillerRunning",
		Name:         "HCity1_GetChillerPlantChillerRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "HCity1_GetChillerPlantCoolingLoad",
		Name:         "HCity1_GetChillerPlantCoolingLoad",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "HCity1_GetChillerPlantWetBulb",
		Name:         "HCity1_GetChillerPlantWetBulb",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "HCity1_GetChillerEnergy1Hour",
		Name:         "HCity1_GetChillerEnergy1Hour",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "HCity1_GetChillerEnergy1Day",
		Name:         "HCity1_GetChillerEnergy1Day",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "HCity1_GetChillerEnergy1Month",
		Name:         "HCity1_GetChillerEnergy1Month",
	})
	
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "HCity1_GetChillerCL",
		Name:         "HCity1_GetChillerCL",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "HCity1_GetChillerDeltaT",
		Name:         "HCity1_GetChillerDeltaT",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "HCity1_GetChillerCoP",
		Name:         "HCity1_GetChillerCoP",
	})


	// calculate delta T, CL, CoP of whole chiller plant
	j.SetDownstream(j.Task("HCity1_GetChillerPlantChillerRunning"), j.Task("HCity1_GetChillerPlantCoolingLoad"))

	// calculate delta T, CL, CoP of individual chiller
	j.SetDownstream(j.Task("HCity1_GetChillerDeltaT"), j.Task("HCity1_GetChillerCL"))
	j.SetDownstream(j.Task("HCity1_GetChillerCL"), j.Task("HCity1_GetChillerCoP"))

	// calculate hourly, daily, monthly energy consumption of individual chiller
	j.SetDownstream(j.Task("HCity1_GetChillerEnergy1Hour"), j.Task("HCity1_GetChillerEnergy1Day"))
	j.SetDownstream(j.Task("HCity1_GetChillerEnergy1Day"), j.Task("HCity1_GetChillerEnergy1Month"))

	j.Run()
	return j
}

func Analytics_Sands() *airflow.Job {

	port, err := strconv.Atoi(os.Getenv("INFLUX_PORT"))
	if err != nil {
		fmt.Println("Port Error")
	}
	neo4jport, err2 := strconv.Atoi(os.Getenv("NEO4J_PORT"))
	if err2 != nil {
		fmt.Println("Port Error")
	}
	k := functions.BaseFunction{
		Database:       os.Getenv("INFLUX_DATABASE"),
		Measurement:    os.Getenv("INFLUX_MEASUREMENT"),
		Host:           os.Getenv("INFLUX_HOST"),
		Port:           port,
		Neo4j_Host:     os.Getenv("NEO4J_HOST"),
		Neo4j_Port:     neo4jport,
		Neo4j_Database: os.Getenv("NEO4J_DATABASE"),
		Neo4j_Username: os.Getenv("NEO4J_USERNAME"),
		Neo4j_Password: os.Getenv("NEO4J_PASSWORD"),
	}

	j := &airflow.Job{
		Name:     "test",
		Schedule: "* * * * *",
	}

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantChillerRunning",
		Name:         "Sands_GetChillerPlantChillerRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantChillerEnergy",
		Name:         "Sands_GetChillerPlantChillerEnergy",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantCoolingLoad",
		Name:         "Sands_GetChillerPlantCoolingLoad",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantCoP",
		Name:         "Sands_GetChillerPlantCoP",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantWetBulb",
		Name:         "Sands_GetChillerPlantWetBulb",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantCTRunning",
		Name:         "Sands_GetChillerPlantCTRunning",
	})
	
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantCHWPRunning",
		Name:         "Sands_GetChillerPlantCHWPRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantCDWPRunning",
		Name:         "Sands_GetChillerPlantCDWPRunning",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantCTEnergy",
		Name:         "Sands_GetChillerPlantCTEnergy",
	})
	
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantCHWPEnergy",
		Name:         "Sands_GetChillerPlantCHWPEnergy",
	})
	
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantCDWPEnergy",
		Name:         "Sands_GetChillerPlantCDWPEnergy",
	})
	
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantTotalEnergy",
		Name:         "Sands_GetChillerPlantTotalEnergy",
	})
	
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerEnergy1Hour",
		Name:         "Sands_GetChillerEnergy1Hour",
	})
	
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerEnergy1Day",
		Name:         "Sands_GetChillerEnergy1Day",
	})
	
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerEnergy1Month",
		Name:         "Sands_GetChillerEnergy1Month",
	})
	
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerDeltaT",
		Name:         "Sands_GetChillerDeltaT",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerCL",
		Name:         "Sands_GetChillerCL",
	})

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerCoP",
		Name:         "Sands_GetChillerCoP",
	})
		
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantEnergy1Hour",
		Name:         "Sands_GetChillerPlantEnergy1Hour",
	})
		
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantEnergy1Day",
		Name:         "Sands_GetChillerPlantEnergy1Day",
	})
		
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerPlantEnergy1Month",
		Name:         "Sands_GetChillerPlantEnergy1Month",
	})
		
	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetCTStatus",
		Name:         "Sands_GetCTStatus",
	})


	// calculate running status of each equipment type
	j.SetDownstream(j.Task("Sands_GetChillerPlantChillerRunning"), j.Task("Sands_GetChillerPlantCTRunning"))
	j.SetDownstream(j.Task("Sands_GetChillerPlantCTRunning"), j.Task("Sands_GetChillerPlantCHWPRunning"))
	j.SetDownstream(j.Task("Sands_GetChillerPlantCHWPRunning"), j.Task("Sands_GetChillerPlantCDWPRunning"))
	
	// calculate total energy of each equipment type
	j.SetDownstream(j.Task("Sands_GetChillerPlantChillerEnergy"), j.Task("Sands_GetChillerPlantCTEnergy"))
	j.SetDownstream(j.Task("Sands_GetChillerPlantCTEnergy"), j.Task("Sands_GetChillerPlantCHWPEnergy"))
	j.SetDownstream(j.Task("Sands_GetChillerPlantCHWPEnergy"), j.Task("Sands_GetChillerPlantCDWPEnergy"))
	j.SetDownstream(j.Task("Sands_GetChillerPlantCDWPEnergy"), j.Task("Sands_GetChillerPlantTotalEnergy"))

	// calculate delta T, CL, CoP of individual chiller
	j.SetDownstream(j.Task("Sands_GetChillerDeltaT"), j.Task("Sands_GetChillerCL"))
	j.SetDownstream(j.Task("Sands_GetChillerCL"), j.Task("Sands_GetChillerCoP"))

	// calculate plant CL, CoP
	j.SetDownstream(j.Task("Sands_GetChillerPlantCoolingLoad"), j.Task("Sands_GetChillerPlantCoP"))

	// calculate hourly, daily, monthly energy consumption of individual chiller
	j.SetDownstream(j.Task("Sands_GetChillerEnergy1Hour"), j.Task("Sands_GetChillerEnergy1Day"))
	j.SetDownstream(j.Task("Sands_GetChillerEnergy1Day"), j.Task("Sands_GetChillerEnergy1Month"))

	// calculate hourly, daily, monthly energy consumption of plant
	j.SetDownstream(j.Task("Sands_GetChillerPlantEnergy1Hour"), j.Task("Sands_GetChillerPlantEnergy1Day"))
	j.SetDownstream(j.Task("Sands_GetChillerPlantEnergy1Day"), j.Task("Sands_GetChillerPlantEnergy1Month"))

	// set down stream
	j.SetDownstream(j.Task("Sands_GetChillerPlantTotalEnergy"), j.Task("Sands_GetChillerPlantCoolingLoad"))
	j.SetDownstream(j.Task("Sands_GetChillerPlantCDWPRunning"), j.Task("Sands_GetChillerPlantChillerEnergy"))
	j.SetDownstream(j.Task("Sands_GetChillerPlantTotalEnergy"), j.Task("Sands_GetChillerPlantCoP"))
	j.SetDownstream(j.Task("Sands_GetChillerEnergy1Month"), j.Task("Sands_GetChillerPlantEnergy1Hour"))

	j.Run()
	return j
}

func Test_Analytics() *airflow.Job {

	port, err := strconv.Atoi(os.Getenv("INFLUX_PORT"))
	if err != nil {
		fmt.Println("Port Error")
	}
	neo4jport, err2 := strconv.Atoi(os.Getenv("NEO4J_PORT"))
	if err2 != nil {
		fmt.Println("Port Error")
	}
	k := functions.BaseFunction{
		Database:       os.Getenv("INFLUX_DATABASE"),
		Measurement:    os.Getenv("INFLUX_MEASUREMENT"),
		Host:           os.Getenv("INFLUX_HOST"),
		Port:           port,
		Neo4j_Host:     os.Getenv("NEO4J_HOST"),
		Neo4j_Port:     neo4jport,
		Neo4j_Database: os.Getenv("NEO4J_DATABASE"),
		Neo4j_Username: os.Getenv("NEO4J_USERNAME"),
		Neo4j_Password: os.Getenv("NEO4J_PASSWORD"),
	}

	j := &airflow.Job{
		Name:     "test",
		Schedule: "* * * * *",
	}

	j.Add(&airflow.Task{
		BaseFunction: k,
		FunctionName: "Sands_GetChillerCoP",
		Name:         fmt.Sprintf("%s_GetChillerEnergy", k.Measurement),
	})

	j.Run()
	return j
}

func Example() {

}
