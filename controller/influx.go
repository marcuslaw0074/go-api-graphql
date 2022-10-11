package controller

import (
	// "fmt"
	"encoding/json"
	"fmt"
	"go-api-grapqhl/goparser"
	"go-api-grapqhl/graph/client"
	"go-api-grapqhl/httputil"
	"go-api-grapqhl/model"
	"go-api-grapqhl/ruleengine"
	"go-api-grapqhl/etlengine"
	"net/http"

	// "sort"
	"github.com/gin-gonic/gin"
)

// Query godoc
// @Summary      query influxDB
// @Description  query influxDB
// @Tags         influxdb
// @Accept       json
// @Produce      json
// @Param        query  body      model.Query  true  "query"
// @Success      200      {object}  []model.Row
// @Failure      400      {object}  httputil.HTTPError
// @Failure      404      {object}  httputil.HTTPError
// @Failure      500      {object}  httputil.HTTPError
// @Router       /influxdb/query [post]
func (c *Controller) QueryInfluxDB(ctx *gin.Context) {
	var res model.Query
	if err := ctx.ShouldBindJSON(&res); err != nil {
		httputil.NewError(ctx, http.StatusBadRequest, err)
		return
	}
	if err := res.Validation(); err != nil {
		httputil.NewError(ctx, http.StatusBadRequest, err)
		return
	}
	series, err := client.InfluxdbQuerySeriess(res.Host, res.Port, res.Database, res.Query)
	if err != nil {
		httputil.NewError(ctx, http.StatusNotFound, err)
		return
	}
	ctx.JSON(http.StatusOK, series)
}

// Query godoc
// @Summary      query influxDB
// @Description  query influxDB
// @Tags         influxdb
// @Accept       json
// @Produce      json
// @Param        query  body      model.RuleEngine  true  "rule_engine"
// @Success      200      {object}  model.NewRow{}
// @Failure      400      {object}  httputil.HTTPError
// @Failure      404      {object}  httputil.HTTPError
// @Failure      500      {object}  httputil.HTTPError
// @Router       /influxdb/ruleengine [post]
func (c *Controller) QueryRuleEngine(ctx *gin.Context) {
	var res model.RuleEngine
	if err := ctx.ShouldBindJSON(&res); err != nil {
		httputil.NewError(ctx, http.StatusBadRequest, err)
		return
	}
	if err := res.ValidationTime(); err != nil {
		httputil.NewError(ctx, http.StatusBadRequest, err)
		return
	} else if err := res.ValidationName(); err != nil {
		httputil.NewError(ctx, http.StatusBadRequest, err)
		return
	}
	fmt.Println(res)
	Fu := &goparser.Function{}
	err := Fu.GenerateFunctions(res.Expression, "test")
	mapping := map[string]string{}
    json.Unmarshal([]byte(res.Mapping), &mapping)
	constMap := map[string]float64{}
	json.Unmarshal([]byte(res.ConstMap), &constMap)
	newMap := map[string]int{}
	for ke := range Fu.Mapping {
		val, exists := mapping[ke]
		if exists {
			newMap[val] = Fu.Mapping[ke]
		} else {
			_, exi := constMap[ke]
			if exi {
				newMap[ke] = Fu.Mapping[ke]
			}
		}
	}
	fmt.Println(newMap, "newMap")
	if err != nil {
		httputil.NewError(ctx, http.StatusNotFound, err)
		return
	}
	_, ress, _ := ruleengine.GenerateTimeseriesNew(res.Host, res.Port, res.Measurement, res.Database,
		res.Name, res.Expression, res.StartTime, res.EndTime, mapping)
	ff, _ := ruleengine.GenerateTimeseriesMap(ress)
	Fu.Mapping = newMap
	values := ruleengine.GenerateNewTimeseries(Fu.CallFunctionByMap, ff, constMap)
	ctx.JSON(http.StatusOK, model.NewRow{
		Name:    res.Measurement,
		Tags:    map[string]string{"id": res.Name},
		Columns: []string{"time", "mean_value"},
		Values:  values,
	})
}

// Query godoc
// @Summary      query influxDB
// @Description  query influxDB
// @Tags         influxdb
// @Accept       json
// @Produce      json
// @Param        query  body      model.EtlEngine  true  "etl_engine"
// @Success      200      {object}  model.NewRow{}
// @Failure      400      {object}  httputil.HTTPError
// @Failure      404      {object}  httputil.HTTPError
// @Failure      500      {object}  httputil.HTTPError
// @Router       /influxdb/etlengine [post]
func (c *Controller) QueryEtlEngine(ctx *gin.Context) {
	var res model.EtlEngine
	if err := ctx.ShouldBindJSON(&res); err != nil {
		httputil.NewError(ctx, http.StatusBadRequest, err)
		return
	}
	if err := res.ValidationTime(); err != nil {
		httputil.NewError(ctx, http.StatusBadRequest, err)
		return
	} else if err := res.ValidationName(); err != nil {
		httputil.NewError(ctx, http.StatusBadRequest, err)
		return
	}
	Fu, err := goparser.InputExpression(res.Expression)
	mapping := map[string]string{}
    json.Unmarshal([]byte(res.Mapping), &mapping)
	constMap := map[string]float64{}
	json.Unmarshal([]byte(res.ConstMap), &constMap)
	newMap := map[string]int{}
	for ke := range Fu.Mapping {
		val, exists := mapping[ke]
		if exists {
			newMap[val] = Fu.Mapping[ke]
		} else {
			_, exi := constMap[ke]
			if exi {
				newMap[ke] = Fu.Mapping[ke]
			}
		}
	}
	if err != nil {
		httputil.NewError(ctx, http.StatusNotFound, err)
		return
	}
	_, ress, _ := etlengine.GenerateTimeseriesNew(res.Host, res.Port, res.Measurement, res.Database,
		res.Name, res.Expression, res.StartTime, res.EndTime, mapping)
	ff, _ := etlengine.GenerateTimeseriesMap(ress)
	Fu.Mapping = newMap
	values, columns := etlengine.GenerateNewTimeseries(Fu.CallFunctionByMap, ff, constMap)
	ctx.JSON(http.StatusOK, model.NewRow{
		Name:    res.Measurement,
		Tags:    map[string]string{"id": res.Name},
		Columns: columns,
		Values:  values,
	})
}