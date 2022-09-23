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

const (
	Calculated string = "Calculated__test"
)
