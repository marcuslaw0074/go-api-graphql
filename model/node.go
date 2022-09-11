package model

import (
	"errors"
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

var (
	ErrNodeNameInvalid = errors.New("node name is empty")
)

func FindNodeByName(name string) (interface{}, error){
	database := "WIIOT"
	measurement := "Utility_3"
	dbUri := "neo4j://192.168.100.214:27687"
	// dbUri := "neo4j://192.168.100.214:27687"
	driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth("neo4j", "test", ""))
	if err != nil {
		panic(err)
	}
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()
	defer driver.Close()
	query := `MATCH (n) where n.database=$database and n.measurement=$measurement and n.name=$name`
	result, err := session.ReadTransaction(client.QueryLabel(query, map[string]interface{}{
		"database":    database,
		"measurement": measurement,
		"name":      name,
	}))
	fmt.Println(result)
}