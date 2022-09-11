package client

import (
	"fmt"
	"time"

	influx "github.com/influxdata/influxdb1-client/v2"
)

type InfluxWriteSchema struct {
	Name string
	Tags map[string]string
	Fields map[string]interface{}
	T time.Time
}

func InfluxdbQuery(query, database string) (influx.Result, error) {
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr: "http://192.168.100.216:18086",
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}
	defer c.Close()
	q := influx.NewQuery(query, database, "")
	if response, err := c.Query(q); err == nil && response.Error() == nil {
		return response.Results[0], nil
	}
	return influx.Result{}, err
}


func InfluxdbWritePoints(points []InfluxWriteSchema, database string) error {
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		// Addr: "http://192.168.100.216:18086",
		Addr: "http://localhost:8086",
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}
	defer c.Close()
	bp, _ := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:  database,
		Precision: "s",
	})
	for _, ele := range points {
		pt, err := influx.NewPoint(ele.Name, ele.Tags, ele.Fields, ele.T)
		if err != nil {
			return err
		} else {
			bp.AddPoint(pt)
		}
	}
	return c.Write(bp)
}
