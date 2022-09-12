package controller

import (
	// "fmt"
	"net/http"
	// "strconv"

	"go-api-grapqhl/httputil"
	"go-api-grapqhl/model"

	"github.com/gin-gonic/gin"
)

// ShowNodeByName godoc
// @Summary      Find node given name and label
// @Description  Find node given name and label
// @Tags         neo4j
// @Accept       json
// @Produce      json
// @Param        database      path      string  true  "Database"
// @Param        measurement   path      string  true  "Measurement"
// @Param        label         path      string  true  "Label"
// @Param        name          path      string  true  "Name"
// @Success      200  {object}  [][]interface{}
// @Failure      400  {object}  httputil.HTTPError
// @Failure      404  {object}  httputil.HTTPError
// @Failure      500  {object}  httputil.HTTPError
// @Router       /neo4j/findnode/{database}/{measurement}/{label}/{name} [get]
func (c *Controller) ShowNodeByName(ctx *gin.Context) {
	name := ctx.Param("name")
	database := ctx.Param("database")
	measurement := ctx.Param("measurement")
	label := ctx.Param("label")
	node, err := model.FindNodeByName(name, database, measurement, label)
	if err != nil {
		httputil.NewError(ctx, http.StatusNotFound, err)
		return
	}
	ctx.JSON(http.StatusOK, node)
}

// ShowAdjNodesByName godoc
// @Summary      Find adj node given name and label
// @Description  Find adj node given name and label
// @Tags         neo4j
// @Accept       json
// @Produce      json
// @Param        database      path      string  true  "Database"
// @Param        measurement   path      string  true  "Measurement"
// @Param        label         path      string  true  "Label"
// @Param        name          path      string  true  "Name"
// @Success      200  {object}  [][]interface{}
// @Failure      400  {object}  httputil.HTTPError
// @Failure      404  {object}  httputil.HTTPError
// @Failure      500  {object}  httputil.HTTPError
// @Router       /neo4j/findadjnodes/{database}/{measurement}/{label}/{name} [get]
func (c *Controller) ShowAdjNodesByName(ctx *gin.Context) {
	name := ctx.Param("name")
	database := ctx.Param("database")
	measurement := ctx.Param("measurement")
	label := ctx.Param("label")
	node, err := model.FindAdjNodesByName(name, database, measurement, label)
	if err != nil {
		httputil.NewError(ctx, http.StatusNotFound, err)
		return
	}
	ctx.JSON(http.StatusOK, node)
}

