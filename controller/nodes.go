package controller

import (
	"fmt"
	"net/http"
	// "strconv"

	"go-api-grapqhl/httputil"
	"go-api-grapqhl/model"

	"github.com/gin-gonic/gin"
)


func (c *Controller) ShowNodeByName(ctx *gin.Context) {
	name := ctx.Param("name")
	database := ctx.Param("database")
	measurement := ctx.Param("measurement")
	label := ctx.Param("label")
	fmt.Println(name)
	node, err := model.FindNodeByName(name, database, measurement, label)
	if err != nil {
		httputil.NewError(ctx, http.StatusNotFound, err)
		return
	}
	ctx.JSON(http.StatusOK, node)
}