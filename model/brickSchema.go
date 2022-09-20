package model

import (
	"log"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type PredefinedPoint struct {
	Label string `json:"label" example:"brick"`
	Name string `json:"name" example:"AHU"`
	Database string `json:"database" example:"ArupDemo"`
	Measurement string `json:"measurement" example:"OTP_RealTime"`
}

func (a PredefinedPoint) Validation() error {
	switch {
	case len(a.Name) == 0 || len(a.Label) == 0 || len(a.Database) == 0 || len(a.Measurement) == 0:
		return ErrNameInvalid
	default:
		return nil
	}
}

func (a PredefinedPoint) Write_Points() error {
	dbUri := "neo4j://localhost:7687"
	driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth("neo4j", "test", ""))
	if err != nil {
		return err
	}
	defer driver.Close()
	item, err := insertItem(driver)
	if err != nil {
		return err
	}
	log.Printf("%v\n", item)
	return nil
}

func insertItem(driver neo4j.Driver) (*PredefinedPoint, error) {
    // Sessions are short-lived, cheap to create and NOT thread safe. Typically create one or more sessions
    // per request in your web application. Make sure to call Close on the session when done.
    // For multi-database support, set sessionConfig.DatabaseName to requested database
    // Session config will default to write mode, if only reads are to be used configure session for
    // read mode.
    session := driver.NewSession(neo4j.SessionConfig{})
    defer session.Close()
    result, err := session.WriteTransaction(createItemFn)
    if err != nil {
        return nil, err
    }
    return result.(*PredefinedPoint), nil
}

func createItemFn(tx neo4j.Transaction) (interface{}, error) {
    records, err := tx.Run("CREATE (n:Item { id: $id, name: $name }) RETURN n.id, n.name", map[string]interface{}{
        "id":   1,
        "name": "Item 1",
    })
    // In face of driver native errors, make sure to return them directly.
    // Depending on the error, the driver may try to execute the function again.
    if err != nil {
        return nil, err
    }
    record, err := records.Single()
    if err != nil {
        return nil, err
    }
    // You can also retrieve values by name, with e.g. `id, found := record.Get("n.id")`
    return &PredefinedPoint{
        Label: record.Values[0].(string),
        Name: record.Values[1].(string),
		Database: record.Values[2].(string),
		Measurement: record.Values[3].(string),
    }, nil
}

func QueryWrite(cypher string, params map[string]interface{}) func(neo4j.Transaction) (interface{}, error) {
	return func (tx neo4j.Transaction) (interface{}, error) {
		records, err := tx.Run(cypher, params)
		// In face of driver native errors, make sure to return them directly.
		// Depending on the error, the driver may try to execute the function again.
		if err != nil {
			return nil, err
		}
		record, err := records.Single()
		if err != nil {
			return nil, err
		}
		// You can also retrieve values by name, with e.g. `id, found := record.Get("n.id")`
		return &PredefinedPoint{
			Label: record.Values[0].(string),
			Name: record.Values[1].(string),
			Database: record.Values[2].(string),
			Measurement: record.Values[3].(string),
		}, nil
	}
}