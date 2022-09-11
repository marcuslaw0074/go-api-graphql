package controller

import (
	// "fmt"
	"net/http"
	"strconv"

	"go-api-grapqhl/httputil"
	"go-api-grapqhl/model"

	"github.com/gin-gonic/gin"
)


func (c *Controller) ShowNodeByName(ctx *gin.Context) {
	name := ctx.Param("name")
	account, err := model.AccountOne(name)
	if err != nil {
		httputil.NewError(ctx, http.StatusNotFound, err)
		return
	}
	ctx.JSON(http.StatusOK, account)
}