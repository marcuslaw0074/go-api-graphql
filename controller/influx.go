package controller

import (
	"net/http"
	"go-api-grapqhl/graph/client"
	"go-api-grapqhl/httputil"
	"go-api-grapqhl/model"
	"github.com/gin-gonic/gin"
)

// Query godoc
// @Summary      query influxDB
// @Description  query influxDB
// @Tags         influxdb
// @Accept       json
// @Produce      json
// @Param        query  body      model.Query  true  "query"
// @Success      200      {object}  model.Row
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
	series, err := client.InfluxdbQuerySeries(res.Host, res.Port, res.Database, res.Query)
	if err != nil {
		httputil.NewError(ctx, http.StatusNotFound, err)
		return
	}
	ctx.JSON(http.StatusOK, model.Row{
		Name: series.Name,
		Tags: series.Tags,
		Columns: series.Columns,
		Values: series.Values,
		Partial: series.Partial,
	})
}

