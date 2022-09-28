package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type ClientDataPoint struct {
	BMS_id string `json:"BMS_id"`
	EquipmentName string `json:"EquipmentName"`
	Interval string `json:"Interval"`
	Level string `json:"Level"`
	Database string `json:"database"`
	Measurement string `json:"measurement"`
	Name string `json:"name"`
	Unit string `json:"unit"`
}

type ClientEquipmentPoint struct {
	Level string `json:"Level"`
	Database string `json:"database"`
	Measurement string `json:"measurement"`
	Name string `json:"name"`
}

type PredefinePoint struct {
	Name  string            `json:"name"`
	Type  string            `json:"type"`
	Value []*PredefinePoint `json:"value"`
}

type TaggingPoint struct {
	BMS_id     string `json:"BMS_id"`
	PointName  string `json:"PointName"`
	System     string `json:"System"`
	SubSystem  string `json:"SubSystem"`
	DeviceType string `json:"DeviceType"`
	DeviceName string `json:"DeviceName"`
	PointType  string `json:"PointType"`
	Location   string `json:"Location"`
	Level      string `json:"Level"`
	ClassType  string `json:"ClassType"`
	Interval   string `json:"Interval"`
	Unit       string `json:"Unit"`
}

func stringInSlice(st string, stList []string) int {
	for ind, ele := range stList {
		if ele == st {
			return ind
		}
	}
	return -1
}

func GeneratePredefineModel(filePath string) ([]string, []string) {
	var pt PredefinePoint
	jsonFile, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
	} else {
		byteValue, _ := ioutil.ReadAll(jsonFile)
		json.Unmarshal(byteValue, &pt)
		ls := []string{fmt.Sprintf("MERGE (a: %s {database: \"%s\", measurement: \"%s\", name: \"%s\"})", "brick", "WIIOT", "Utility_3", "System")}
		lss := make([]string, 0)
		names := make([]string, 0)
		for ind, ele := range pt.Value {
			if stringInSlice(ele.Name, names) < 0 {
				ls = append(ls, fmt.Sprintf("MERGE (a%v: %s {database: \"%s\", measurement: \"%s\", name: \"%s\"})", ind, "brick", "WIIOT", "Utility_3", ele.Name))
			}
			lss = append(lss, fmt.Sprintf(`
				MERGE (a: %s {database: "%s", measurement: "%s", name: "%s"}) MERGE (b: %s {database: "%s", measurement: "%s", name: "%s"}) MERGE (a)-[:%s]->(b)`, "brick", "WIIOT", "Utility_3", ele.Name, "brick", "WIIOT", "Utility_3", "System", "subClassOf"))
			for ind2, ele2 := range ele.Value {
				if stringInSlice(ele.Name, names) < 0 {
					ls = append(ls, fmt.Sprintf("MERGE (a%v%v: %s {database: \"%s\", measurement: \"%s\", name: \"%s\"})", ind, ind2, "brick", "WIIOT", "Utility_3", ele2.Name))
				}
				lss = append(lss, fmt.Sprintf(`
					MERGE (a: %s {database: "%s", measurement: "%s", name: "%s"}) MERGE (b: %s {database: "%s", measurement: "%s", name: "%s"}) MERGE (a)-[:%s]->(b)`, "brick", "WIIOT", "Utility_3", ele2.Name, "brick", "WIIOT", "Utility_3", ele.Name, "subClassOf"))
				for ind3, ele3 := range ele2.Value {
					if stringInSlice(ele.Name, names) < 0 {
						ls = append(ls, fmt.Sprintf("MERGE (a%v%v%v: %s {database: \"%s\", measurement: \"%s\", name: \"%s\"})", ind, ind2, ind3, "brick", "WIIOT", "Utility_3", ele3.Name))
					}
					lss = append(lss, fmt.Sprintf(`
						MERGE (a: %s {database: "%s", measurement: "%s", name: "%s"}) MERGE (b: %s {database: "%s", measurement: "%s", name: "%s"}) MERGE (a)-[:%s]->(b)`, "brick", "WIIOT", "Utility_3", ele3.Name, "brick", "WIIOT", "Utility_3", ele2.Name, "isPartOf"))
					lss = append(lss, fmt.Sprintf(`
						MERGE (a: %s {database: "%s", measurement: "%s", name: "%s"}) MERGE (b: %s {database: "%s", measurement: "%s", name: "%s"}) MERGE (a)-[:%s]->(b)`, "brick", "WIIOT", "Utility_3", ele2.Name, "brick", "WIIOT", "Utility_3", ele3.Name, "hasPart"))
					for ind4, ele4 := range ele3.Value {
						if stringInSlice(ele.Name, names) < 0 {
							ls = append(ls, fmt.Sprintf("MERGE (a%v%v%v%v: %s {database: \"%s\", measurement: \"%s\", name: \"%s\"})", ind, ind2, ind3, ind4, "brick", "WIIOT", "Utility_3", ele4.Name))
						}
						lss = append(lss, fmt.Sprintf(`
							MERGE (a: %s {database: "%s", measurement: "%s", name: "%s"}) MERGE (b: %s {database: "%s", measurement: "%s", name: "%s"}) MERGE (a)-[:%s]->(b)`, "brick", "WIIOT", "Utility_3", ele4.Name, "brick", "WIIOT", "Utility_3", ele3.Name, "isPartOf"))
						lss = append(lss, fmt.Sprintf(`
							MERGE (a: %s {database: "%s", measurement: "%s", name: "%s"}) MERGE (b: %s {database: "%s", measurement: "%s", name: "%s"}) MERGE (a)-[:%s]->(b)`, "brick", "WIIOT", "Utility_3", ele3.Name, "brick", "WIIOT", "Utility_3", ele4.Name, "hasPart"))
					}
				}
			}
		}
		return ls, lss
	}
	return []string{}, []string{}
}

func AddClientPoint(uri, username, password, database, measurement string, point TaggingPoint, labels ...string) error {
	driver, err := neo4j.NewDriver(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		return err
	}
	defer driver.Close()
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()


	return WriteClientPoint(session, point, database, measurement, labels...)
}

func labelToString(label ...string) string {
	var a string = ""
	for _, ele := range label {
		a = fmt.Sprintf(":%s %s", ele, a)
	}
	return a
}

func WriteClientPoint(session neo4j.Session, point TaggingPoint, database, measurement string, labels ...string) error {

	var brick string
	var bldg string
	if point.ClassType != "Class" {
		brick = "elecbrick"
		bldg = "elecbldg"
	} else {
		brick = "brick"
		bldg = "bldg"
	}
	newLabel := labelToString(labels...)
	cypher := fmt.Sprintf(`
			MERGE (a: %s %s {BMS_id: $BMS_id, EquipmentName: $DeviceName, Interval: $Interval, Level: $Level, database: $database, measurement: $measurement, name: $PointName, unit: $unit})
			MERGE (b: %s {Level: $Level, database: $database, measurement: $measurement, name: $DeviceName})
			MERGE (c: %s {database: $database, measurement: $measurement, name: $PointType})
			MERGE (d: %s {database: $database, measurement: $measurement, name: $DeviceType})
			MERGE (e: %s {database: $database, measurement: $measurement, name: $Level})
			MERGE (a)-[:isPointOf]->(c)
			MERGE (c)-[:hasPoint]->(a)
			MERGE (a)-[:isPartOf]->(b)
			MERGE (b)-[:hasPart]->(a)
			MERGE (c)-[:isPartOf]->(d)
			MERGE (d)-[:hasPart]->(c)
			MERGE (d)-[:hasPoint]->(b)
			MERGE (b)-[:isPointOf]->(d)
			MERGE (b)-[:hasLocation]->(e)
			MERGE (e)-[:isLocationOf]->(b)
			RETURN a
			`, bldg, newLabel, bldg, brick, brick, bldg)
	fmt.Println(cypher)
	res, err := session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
		result, err := transaction.Run(
			cypher,
			map[string]interface{}{
				"BMS_id": point.BMS_id,
				"DeviceName": point.DeviceName,
				"DeviceType": point.DeviceType,
				"Interval": point.Interval,
				"PointName": point.PointName,
				"PointType": point.PointType,
				"unit": point.Unit,
				"Level": point.Level,
				"database": database,
				"measurement": measurement,
			})
		if err != nil {
			return nil, err
		}

		if result.Next() {
			return result.Record().Values[0], nil
		}

		return nil, result.Err()
	})
	if err != nil {
		return err
	}
	fmt.Println(res)
	return nil
}

func Query(query string, params map[string]interface{}) func (neo4j.Transaction) (interface{}, error) {
	return func(tx neo4j.Transaction) (interface{}, error) {
		records, err := tx.Run(query, params)
		if err != nil {
			return nil, err
		}
		s2 := make([][]string, 0)
		for records.Next() {
			res, _ := records.Record().Get("o")
			res2, _ := records.Record().Get("s")
			s2 = append(s2, []string{res.(string), res2.(string)})
		}
		if err != nil {
			return nil, err
		}
		return s2, nil
	}
}

func QueryNew(query string, params map[string]interface{}) func (neo4j.Transaction) (interface{}, error) {
	return func(tx neo4j.Transaction) (interface{}, error) {
		records, err := tx.Run(query, params)
		if err != nil {
			return nil, err
		}
		s := make([]interface{}, 0)
		s2 := make([][]interface{}, 0)
		for records.Next() {
			res := records.Record().Values
			s2 = append(s2, res)
			s = append(s, res[0])
		}
		if err != nil {
			return nil, err
		}
		return s2, nil
	}
}

func QueryLabel(query string, params map[string]interface{}) func (neo4j.Transaction) (interface{}, error) {
	return func(tx neo4j.Transaction) (interface{}, error) {
		records, err := tx.Run(query, params)
		if err != nil {
			return nil, err
		}
		s2 := make([]string, 0)
		for records.Next() {
			res, _ := records.Record().Get("name")
			s2 = append(s2, res.(string))
			log.Printf("The current record is: %v\n", res.(string))
		}
		if err != nil {
			return nil, err
		}
		return s2, nil
	}
}

func QueryLabelValue(query string, params map[string]interface{}) func (neo4j.Transaction) (interface{}, error) {
	return func(tx neo4j.Transaction) (interface{}, error) {
		records, err := tx.Run(query, params)
		if err != nil {
			return nil, err
		}
		s2 := make([][]string, 0)
		s := make([]interface{}, 0)
		for records.Next() {
			s = append(s, records.Record())
			res, _ := records.Record().Get("label")
			res2, _ := records.Record().Get("value")
			s2 = append(s2, []string{res.(string), res2.(string)})
			log.Printf("The current record is: %v\n", res.(string))
		}
		if err != nil {
			return nil, err
		}
		return s2, nil
	}
}
