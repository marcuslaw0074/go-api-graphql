package functions

import (
	logging "go-api-grapqhl/log"
)

type BaseFunction struct {
	Host        string
	Port        int
	Database    string
	Measurement string
}

const (
	Calculated string = "Calculated__test"
)

var Logger = logging.StartLogger("LogFile.log")

