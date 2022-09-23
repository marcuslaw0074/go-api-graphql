package model

import (
	"errors"
	"fmt"
	"go-api-grapqhl/graph/client"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type ClientPoint struct {
	BMS_id string `json:"BMS_id"`
	EquipmentName string `json:"EquipmentName"`
	Interval string `json:"Interval"`
	Level string `json:"Level"`
	Database string `json:"database"`
	Measurement string `json:"measurement"`
	Name string `json:"name"`
	Unit string `json:"unit"`
}

type ClientEquipment struct {
	Level string `json:"Level"`
	Database string `json:"database"`
	Measurement string `json:"measurement"`
	Name string `json:"name"`
}

type BrickPoint struct {
	Database string `json:"database"`
	Measurement string `json:"measurement"`
	Name string `json:"name"`
}

var (
	ErrNodeNameInvalid = errors.New("node name is empty")
)

func FindNodeByName(name, database, measurement, label string) ([][]interface{}, error){
	dbUri := "neo4j://192.168.100.214:27687"
	driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth("neo4j", "test", ""))
	if err != nil {
		panic(err)
	}
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()
	defer driver.Close()
	query := fmt.Sprintf(`MATCH (n: %s) where n.database=$database and n.measurement=$measurement and n.name=$name return n`, label)
	result, err := session.ReadTransaction(client.QueryNew(query, map[string]interface{}{
		"database":    database,
		"measurement": measurement,
		"name":      name,
	}))

	return result.([][]interface{}), nil
}

func FindAdjNodesByName(name, database, measurement, label string) ([][]interface{}, error){
	dbUri := "neo4j://192.168.100.214:27687"
	driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth("neo4j", "test", ""))
	if err != nil {
		panic(err)
	}
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()
	defer driver.Close()
	query := fmt.Sprintf(`MATCH (n: %s)-[r]-(p) where n.database=$database and n.measurement=$measurement and n.name=$name return n, r, p`, label)
	result, err := session.ReadTransaction(client.QueryNew(query, map[string]interface{}{
		"database":    database,
		"measurement": measurement,
		"name":      name,
	}))
	return result.([][]interface{}), nil
}