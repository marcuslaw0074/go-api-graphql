package model

import (
	"errors"
	"regexp"
	"strings"
)

// Query example
type Query struct {
	Query string `json:"query" example:"SELECT * FROM Utility_3 LIMIT 1"`
	Database string `json:"database" example:"WIIOT"`
	Host string `json:"host" example:"192.168.100.216"`
	Port int `json:"port" example:"18086" format:"int64"`
}

var (
	ErrQueryInvalid = errors.New("only select statement is allowed!")
)

// Validation example
func (a Query) Validation() error {
	res, _ := regexp.MatchString(`^select`, strings.ToLower(a.Query))
	switch {
	case len(a.Query) == 0:
		return ErrNameInvalid
	case !res:
		return ErrQueryInvalid
	default:
		return nil
	}
}

type Row struct {
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns,omitempty"`
	Values  [][]interface{}   `json:"values,omitempty"`
	Partial bool              `json:"partial,omitempty"`
}