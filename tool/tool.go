package tool

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func sliceFilledWithString(size int, str string) []string {
	data := make([]string, size)
	for i := 0; i < size; i++ {
		data[i] = str
	}
	return data
}

type AllDataframe struct {
	EquipmentName string
	FunctionType  string
	Id            string
	Dataframe     dataframe.DataFrame
}

func findElementByEquip(s []AllDataframe, equipment string) (int, error) {
	for ind, ele := range s {
		if ele.EquipmentName == equipment {
			return ind, nil
		}
	}
	return -1, errors.New("cannot find dataframe")
}

func IntContains(list []int, str int) (int, bool) {
	for index, a := range list {
		if a == str {
			return index, true
		}
	}
	return -1, false
}

func StrContains(list []string, str string) int {
	for index, a := range list {
		if a == str {
			return index
		}
	}
	return -1
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

type AllGroupDataframe struct {
	Block         string
	BuildingName  string
	EquipmentName string
	FunctionType  string
	Id            string
	Prefername    string
	Dataframe     dataframe.DataFrame
}

func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func FindEleByEquip(s []GroupDataframe, equipment string) (int, error) {
	for ind, ele := range s {
		if ele.EquipmentName == equipment {
			return ind, nil
		}
	}
	return -1, errors.New("cannot find dataframe")
}

func FindGroupByList(groupBy ...string) []string {
	originLs := []string{"Block", "BuildingName", "EquipmentName", "FunctionType", "id", "prefername"}
	ls := []string{"", "", "", "", "", ""}
	for ind, ele := range originLs {
		for _, el := range groupBy {
			if ele == el {
				ls[ind] = ele
			}
		}
	}
	return ls
}

func FindEleByEquipAll(s []AllGroupDataframe, equipment string) (int, error) {
	for ind, ele := range s {
		if ele.EquipmentName == equipment {
			return ind, nil
		}
	}
	return -1, errors.New("cannot find dataframe")
}

func ConcatDataframe(s []GroupDataframe) (dataframe.DataFrame, error) {
	if len(s) == 0 {
		return dataframe.New(), nil
	}
	df := s[0].Dataframe
	equip := s[0].EquipmentName
	ls := df.Names()
	for ind, ele := range ls {
		if ele != "Time" {
			ls[ind] = fmt.Sprintf("%s_%s", equip, ele)
		}
	}
	err := df.SetNames(ls...)
	if err == nil {
		for _, ele := range s[1:] {
			dfNew := ele.Dataframe
			equipNew := ele.EquipmentName
			ls := dfNew.Names()
			for ind, ele := range ls {
				if ele != "Time" {
					ls[ind] = fmt.Sprintf("%s_%s", equipNew, ele)
				}
			}
			err := dfNew.SetNames(ls...)
			if err == nil {
				df = df.InnerJoin(dfNew, "Time")
			} else {
				return df, err
			}
		}
		return df, nil
	} else {
		return dataframe.New(), err
	}
}

// return true if f contains NaN
func ContainNaN(f []float64) bool {
	for _, ele := range f {
		if math.IsNaN(ele) {
			return true
		}
	}
	return false
}

// return true if f contains NaN
func GetNonNan(f []float64) []float64 {
	ls := make([]float64, 0)
	for _, ele := range f {
		if !math.IsNaN(ele) {
			ls = append(ls, ele)
		}
	}
	return ls
}

func SumListStatus(f []float64) float64 {
	ls := 0.0
	for _, ele := range f {
		if ele >= 0.5 {
			ls ++
		}
	}
	return ls
}

func SumList(f []float64) float64 {
	ls := 0.0
	for _, ele := range f {
		if ele > 0 {
			ls = ls + ele
		}
	}
	return ls
}

func GetCurrenttimeString() string {
	now := time.Now()
	currentYear, currentMonth, _ := now.Date()
	currentLocation := now.Location()
	firstOfMonth := time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, currentLocation)
	return firstOfMonth.Format("2006-01-02T15:04:05Z")
}
