package graph

import (
	"fmt"
	"io"
	"strconv"
)

type AggregationsType string

const (
	count AggregationsType = "COUNT"
	sum   AggregationsType = "SUM"
	mean  AggregationsType = "MEAN"
	max   AggregationsType = "MAX"
	min   AggregationsType = "MIN"
	first AggregationsType = "FIRST"
	last  AggregationsType = "LAST"
)

var AllAggregationsType = []AggregationsType{
	count,
	sum,
	mean,
	max,
	min,
	first,
	last,
}



func (e AggregationsType) IsValid() bool {
	switch e {
	case count, sum, mean, max, min, first, last:
		return true
	}
	return false
}

func (e *AggregationsType) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}

	*e = AggregationsType(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid TodoStatus", str)
	}
	return nil
}

func (e AggregationsType) String() string {
	return string(e)
}

func (e AggregationsType) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}
