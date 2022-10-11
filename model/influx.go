package model

import (
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/influxdata/influxdb1-client/models"
)

// Query example
type Query struct {
	Query    string `json:"query" example:"SELECT * FROM Utility_3 LIMIT 1"`
	Database string `json:"database" example:"WIIOT"`
	Host     string `json:"host" example:"192.168.100.216"`
	Port     int    `json:"port" example:"18086" format:"int64"`
}

type SetRuleTable struct {
	Host  string `json:"host" example:"192.168.100.214"`
	Port  int    `json:"port" example:"36379" format:"int64"`
	Key   string `json:"key" example:"ruleTable"`
	Value string `json:"value" example:"[]"`
}

type GetRuleTable struct {
	Host  string `json:"host" example:"192.168.100.214"`
	Port  int    `json:"port" example:"36379" format:"int64"`
	Key   string `json:"key" example:"ruleTable"`
}

type IsSuccess struct {
	Success bool   `json:"success" example:"true" format:"bool"`
	Time    string `json:"time" example:"2022-10-04T00:00:00Z"`
	Result string `json:"result" example:"example"`
}

// RuleEngine example
type RuleEngine struct {
	EndTime     string `json:"endTime" example:"'2018-04-02T15:04:05.000Z'"`
	StartTime   string `json:"startTime" example:"'2018-04-01T15:04:05.000Z'"`
	Expression  string `json:"expression" example:"a+b"`
	Name        string `json:"name" example:"Rule_1"`
	Database    string `json:"database" example:"Disney"`
	Measurement string `json:"measurement" example:"hkdl"`
	Host        string `json:"host" example:"18.163.30.4"`
	Port        int    `json:"port" example:"8086" format:"int64"`
	Mapping     string `json:"mapping" example:"{\"a\":\"CCP1 CH2 Supply Temp.(Deg C)\",\"b\":\"CCP1 CH2 Return Temp.(Deg C)\"}"`
	ConstMap    string `json:"constmap" example:"{\"c\":4.2}"`
}

// EtlEngine example
type EtlEngine struct {
	EndTime     string `json:"endTime" example:"'2018-04-02T15:04:05.000Z'"`
	StartTime   string `json:"startTime" example:"'2018-04-01T15:04:05.000Z'"`
	Expression  string `json:"expression" example:"(c<b-a)"`
	Name        string `json:"name" example:"Rule_1"`
	Database    string `json:"database" example:"Disney"`
	Measurement string `json:"measurement" example:"hkdl"`
	Host        string `json:"host" example:"18.163.30.4"`
	Port        int    `json:"port" example:"8086" format:"int64"`
	Mapping     string `json:"mapping" example:"{\"a\":\"CCP1 CH2 Supply Temp.(Deg C)\",\"b\":\"CCP1 CH2 Return Temp.(Deg C)\"}"`
	ConstMap    string `json:"constmap" example:"{\"c\":1}"`
}

type RuleEngineResultTemp struct {
	Ids     []string     `json:"ids" example:"[]"`
	Results []models.Row `json:"results" example:"[]"`
}

type NewRow struct {
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns,omitempty"`
	Values  [][]interface{}   `json:"values,omitempty"`
	Partial bool              `json:"partial,omitempty"`
}

var (
	ErrQueryInvalid        = errors.New("only select statement is allowed!")
	ErrVariableNameInvalid = errors.New("name is invalid!")
)

// Validation example
func (r RuleEngine) ValidationTime() error {
	layout := "2006-01-02T15:04:05.000Z"
	start := r.StartTime
	if strings.Contains(start, "'") {
		start = strings.Replace(start, "'", "", -1)
		_, err := time.Parse(layout, start)
		if err != nil {
			return err
		}
	} else {
		res, err := regexp.MatchString(`now\([\d]+(m|d)\)`, strings.ToLower(start))
		if err != nil {
			return err
		} else if res {
			return nil
		} else {
			return errors.New("time string doesnt match 'now'")
		}
	}
	end := r.EndTime
	if strings.Contains(end, "'") {
		end = strings.Replace(end, "'", "", -1)
		_, err := time.Parse(layout, end)
		if err != nil {
			return err
		}
	} else {
		res, err := regexp.MatchString(`now\([\d]+(m|d)\)`, strings.ToLower(end))
		if err != nil {
			return err
		} else if res {
			return nil
		} else {
			return errors.New("time string doesnt match 'now'")
		}
	}
	return nil
}

func (r RuleEngine) ValidationName() error {
	res, _ := regexp.MatchString(`^[_a-zA-Z]\w*`, r.Name)
	switch {
	case len(r.Name) == 0:
		return ErrNameInvalid
	case !res:
		return ErrVariableNameInvalid
	default:
		return nil
	}
}

// Validation example
func (r EtlEngine) ValidationTime() error {
	layout := "2006-01-02T15:04:05.000Z"
	start := r.StartTime
	if strings.Contains(start, "'") {
		start = strings.Replace(start, "'", "", -1)
		_, err := time.Parse(layout, start)
		if err != nil {
			return err
		}
	} else {
		res, err := regexp.MatchString(`now\([\d]+(m|d)\)`, strings.ToLower(start))
		if err != nil {
			return err
		} else if res {
			return nil
		} else {
			return errors.New("time string doesnt match 'now'")
		}
	}
	end := r.EndTime
	if strings.Contains(end, "'") {
		end = strings.Replace(end, "'", "", -1)
		_, err := time.Parse(layout, end)
		if err != nil {
			return err
		}
	} else {
		res, err := regexp.MatchString(`now\([\d]+(m|d)\)`, strings.ToLower(end))
		if err != nil {
			return err
		} else if res {
			return nil
		} else {
			return errors.New("time string doesnt match 'now'")
		}
	}
	return nil
}

func (r EtlEngine) ValidationName() error {
	res, _ := regexp.MatchString(`^[_a-zA-Z]\w*`, r.Name)
	switch {
	case len(r.Name) == 0:
		return ErrNameInvalid
	case !res:
		return ErrVariableNameInvalid
	default:
		return nil
	}
}

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
