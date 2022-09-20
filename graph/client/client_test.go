package client

import (
	"testing"
)

func TestClientPoints(t *testing.T) {
    err := AddClientPoint("neo4j://localhost:7687", "neo4j", "test",
	 "WIIOT", "Utility_3", false, TaggingPoint{
		BMS_id: "UT3_CH01_Indi_Flow",
		PointName: "UT3_CH01_Indi_Flow",
		System: "HVAC_System",
		SubSystem: "Water_System",
		DeviceType: "Chiller",
		DeviceName: "CH01",
		PointType: "Chiller_Water_Flowrate",
		Location: "Building",
		Level: "UT3",
		ClassType: "Class",
		Interval: "20T",
		Unit: "°C",

	}, []string{"bldg"}...)
    if err != nil {
        t.Fatal("got errors:", err)
    }
}