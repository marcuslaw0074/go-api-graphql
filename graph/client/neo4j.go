package client

import (
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

func Query(query string, params map[string]interface{}) func (neo4j.Transaction) (interface{}, error) {
	return func(tx neo4j.Transaction) (interface{}, error) {
		records, err := tx.Run(query, params)
		if err != nil {
			return nil, err
		}
		s2 := make([][]string, 0)
		s := make([]interface{}, 0)
		for records.Next() {
			s = append(s, records.Record())
			res, _ := records.Record().Get("o")
			res2, _ := records.Record().Get("s")
			s2 = append(s2, []string{res.(string), res2.(string)})
			fmt.Printf("The current record is: %v, %v \n", res.(string), res2.(string))
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
		s := make([]interface{}, 0)
		for records.Next() {
			s = append(s, records.Record())
			res, _ := records.Record().Get("name")
			s2 = append(s2, res.(string))
			fmt.Printf("The current record is: %v\n", res.(string))
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
			fmt.Printf("The current record is: %v\n", res.(string))
		}
		if err != nil {
			return nil, err
		}
		return s2, nil
	}
}
