package airflow

import (
	// "encoding/json"
	"errors"
	"fmt"
	"go-api-grapqhl/graph/client"
	"math"
	"sort"
	// "strings"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	// "time"
)

type BaseFunction struct{}

type Timeseries struct {
	Time  string  `json:"time"`
	Value float64 `json:"value" format:"float64"`
}

type AllDataframe struct {
	EquipmentName string
	FunctionType  string
	Id            string
	Dataframe     dataframe.DataFrame
}

func IntContains(list []int, str int) (int, bool) {
	for index, a := range list {
		if a == str {
			return index, true
		}
	}
	return -1, false
}

func sliceFilledWithString(size int, str string) []string {
	data := make([]string, size)
	for i := 0; i < size; i++ {
		data[i] = str
	}
	return data
}

func ApplyFunction(function func(...float64) float64, indCol ...int) func(series.Series) series.Series {
	return func(s series.Series) series.Series {
		floats := s.Float()
		list := make([]float64, 0)
		for index, value := range floats {
			_, contains := IntContains(indCol, index)
			if contains {
				list = append(list, value)
			}
		}
		return series.Floats(function(list...))
	}
}

func findElementByEquip(s []AllDataframe, equipment string) (int, error) {
	for ind, ele := range s {
		if ele.EquipmentName == equipment {
			return ind, nil
		}
	}
	return -1, errors.New("cannot find dataframe")
}

func RemoveIndexs(s []float64, indexs ...int) []float64 {
	sort.Ints(indexs)
	for ind := len(indexs) - 1; ind >= 0; ind-- {
		s = append(s[:ind], s[ind+1:]...)
	}
	return s
}

func GetKeys(m map[int]float64) []int {
	keys := make([]int, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}

func AppendIndexs(s []float64, values map[int]float64) []float64 {
	indexs := GetKeys(values)
	sort.Ints(indexs)
	for ind := len(indexs) - 1; ind >= 0; ind-- {
		c, exists := values[ind]
		if exists {
			s = append(s[:ind+1], s[ind:]...)
			s[ind] = c
		}
	}
	return s
}

func findEleByEquip(s []GroupDataframe, equipment string) (int, error) {
	for ind, ele := range s {
		if ele.EquipmentName == equipment {
			return ind, nil
		}
	}
	return -1, errors.New("cannot find dataframe")
}

func ShiftValue(df dataframe.DataFrame, freq int, col string) dataframe.DataFrame {
	ser := df.Col(col).Copy().Float()
	if freq > 0 {
		ls := make([]float64, 0)
		for i := 0; i < freq; i++ {
			ls = append(ls, math.NaN())
		}
		ser = append(ls, ser[freq:]...)
	} else if freq < 0 {
		ls := make([]float64, 0)
		for i := 0; i < -freq; i++ {
			ls = append(ls, math.NaN())
		}
		ser = append(ser[:len(ser)+freq], ls...)
	} else {
		return df
	}
	return df.Mutate(series.New(ser, series.Float, "Shift")).Drop(col)
}

func DiffValue(df dataframe.DataFrame, freq int, col string) dataframe.DataFrame {
	ser := df.Copy().Col(col).Float()
	serr := make([]float64, 0)
	if freq > 0 {
		ls := make([]float64, 0)
		for i := 0; i < freq; i++ {
			ls = append(ls, math.NaN())
		}
		serr = append(ls, ser[:len(ser)-freq]...)
		for ind, ele := range serr {
			serr[ind] = ser[ind] - ele
		}
	} else if freq < 0 {
		ls := make([]float64, 0)
		for i := 0; i < -freq; i++ {
			ls = append(ls, math.NaN())
		}
		serr = append(ser[-freq:], ls...)
		for ind, ele := range serr {
			serr[ind] = ser[ind] - ele
		}
	} else {
		return df
	}
	return df.Copy().Drop(col).Mutate(series.New(serr, series.Float, col))
}

type GroupDataframe struct {
	EquipmentName string
	Dataframe     dataframe.DataFrame
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func (f BaseFunction) Test() (string, error) {
	newFunctionType := "Chiller_Delta_T"
	measurement := "Utility_3"
	database := "WIIOT"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Chilled_Water_Return_Temperature_Sensor' OR
			"FunctionType"='Chiller_Chilled_Water_Supply_Temperature_Sensor' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, measurement)
	dfGroup := client.QueryDfGroup(query, database)
	lss := []int{1, 2}
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(ApplyFunction(func(f ...float64) float64 {
			return f[0] - f[1]
		}, lss...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		lsss := client.WriteDfGroup(query, database, measurement, ele.EquipmentName, newFunctionType, df)
		t := client.InfluxdbWritePoints(lsss, "WIIOT")
		fmt.Println(t)
	}
	return "ok", nil
}

func (f BaseFunction) Test2() (string, error) {
	newFunctionType := "Chiller_Delta_T"
	measurement := "Utility_3"
	database := "WIIOT"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, measurement)
	dfGroup := client.QueryDfGroup(query, database)
	for _, ele := range dfGroup {
		df := DiffValue(ele.Dataframe, 1, "Chiller_Power_Sensor").Rename("Value", "Chiller_Power_Sensor")
		lsss := client.WriteDfGroup(query, database, measurement, ele.EquipmentName, newFunctionType, df)
		t := client.InfluxdbWritePoints(lsss, "WIIOT")
		fmt.Println(t)
	}
	return "ok", nil
}

func (f BaseFunction) Test3() (string, error) {
	newFunctionType := "Chiller_Delta_T"
	measurement := "Utility_3"
	database := "WIIOT"
	query := fmt.Sprintf(`SELECT MEAN(value) FROM %s 
			WHERE "FunctionType"='Chiller_Power_Sensor' AND 
			time>now()-60m GROUP BY EquipmentName, FunctionType, id, time(20m)`, measurement)
	dfGroup := client.QueryDfGroup(query, database)
	for _, ele := range dfGroup {
		df := ele.Dataframe.Rapply(ApplyFunction(func(f ...float64) float64 {
			return f[0] / 1000
		}, []int{1}...)).Rename("Value", "X0").Mutate(ele.Dataframe.Col("Time"))
		lsss := client.WriteDfGroup(query, database, measurement, ele.EquipmentName, newFunctionType, df)
		t := client.InfluxdbWritePoints(lsss, "WIIOT")
		fmt.Println(t)
	}
	return "ok", nil
}
