package controller

import (
	"fmt"
	"go-api-grapqhl/httputil"
	"go-api-grapqhl/model"
	"net/http"

	"github.com/gin-gonic/gin"
	redis "github.com/go-redis/redis/v9"
)

// Query godoc
// @Summary      query influxDB
// @Description  query influxDB
// @Tags         redis
// @Accept       json
// @Produce      json
// @Param        query  body      model.SetRuleTable  true  "query"
// @Success      200      {object}  model.IsSuccess
// @Failure      400      {object}  httputil.HTTPError
// @Failure      404      {object}  httputil.HTTPError
// @Failure      500      {object}  httputil.HTTPError
// @Router       /redis/set [post]
func (c *Controller) SetRuleTable(ctx *gin.Context) {
	var res model.SetRuleTable
	if err := ctx.ShouldBindJSON(&res); err != nil {
		httputil.NewError(ctx, http.StatusBadRequest, err)
		return
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%v", res.Host, res.Port),
		Password: "",
		DB:       0,
	})
	err := rdb.Set(ctx, res.Key, res.Value, 0).Err()
	if err != nil {
		httputil.NewError(ctx, http.StatusNotFound, err)
		return
	}
	ctx.JSON(http.StatusOK, map[string]interface{}{
		"success": true,
		"time":    "now()",
	})
}

// Query godoc
// @Summary      query influxDB
// @Description  query influxDB
// @Tags         redis
// @Accept       json
// @Produce      json
// @Param        query  body      model.GetRuleTable  true  "query"
// @Success      200      {object}  model.IsSuccess
// @Failure      400      {object}  httputil.HTTPError
// @Failure      404      {object}  httputil.HTTPError
// @Failure      500      {object}  httputil.HTTPError
// @Router       /redis/get [post]
func (c *Controller) GetRuleTable(ctx *gin.Context) {
	var res model.GetRuleTable
	if err := ctx.ShouldBindJSON(&res); err != nil {
		httputil.NewError(ctx, http.StatusBadRequest, err)
		return
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%v", res.Host, res.Port),
		Password: "",
		DB:       0,
	})
	val, err := rdb.Get(ctx, res.Key).Result()
	if err != nil {
		httputil.NewError(ctx, http.StatusNotFound, err)
		return
	}
	ctx.JSON(http.StatusOK, map[string]interface{}{
		"success": true,
		"time":    "now()",
		"result": val,
	})
}
