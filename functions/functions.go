package functions

type BaseFunction struct {
	Host           string
	Port           int
	Database       string
	Measurement    string
	Neo4j_Host     string
	Neo4j_Port     int
	Neo4j_Database string
	Neo4j_Username string
	Neo4j_Password string
}

type Interval struct {
	starttime string
	endTime   string
}

const (
	Calculated string = "Calculated"
)

var timeClause string = "time>'2021-07-31T00:00:00Z' and time<'2021-10-31T00:00:00Z'"

var timeClauseMonth []Interval = []Interval{
	{"2021-08-01T00:00:00Z", "2021-08-31T00:00:00Z"},
	{"2021-09-01T00:00:00Z", "2021-09-30T00:00:00Z"},
}