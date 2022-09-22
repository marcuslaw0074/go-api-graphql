package functions

type BaseFunction struct {
	Host        string
	Port        int
	Database    string
	Measurement string
}

const (
	Calculated string = "Calculated__test"
)

