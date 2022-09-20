package client

import (
	"log"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	// "github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
)

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
