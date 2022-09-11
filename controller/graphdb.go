package controller

// import (
// 	"fmt"
// 	"net/http"
// 	"strconv"

// 	"go-api-grapqhl/httputil"
// 	"go-api-grapqhl/model"

// 	"github.com/gin-gonic/gin"
// )

// AddAccount godoc
// @Summary      Add an account
// @Description  add by json account
// @Tags         accounts
// @Accept       json
// @Produce      json
// @Param        account  body      model.AddAccount  true  "Add account"
// @Success      200      {object}  model.Account
// @Failure      400      {object}  httputil.HTTPError
// @Failure      404      {object}  httputil.HTTPError
// @Failure      500      {object}  httputil.HTTPError
// @Router       /accounts [post]

// func (c *Controller) CreateNode(ctx *gin.Context) {
// 	var addPredefinedPoint model.PredefinedPoint
// 	if err := ctx.ShouldBindJSON(&addPredefinedPoint); err != nil {
// 		httputil.NewError(ctx, http.StatusBadRequest, err)
// 		return
// 	}
// 	if err := addPredefinedPoint.Validation(); err != nil {
// 		httputil.NewError(ctx, http.StatusBadRequest, err)
// 		return
// 	}
// 	point := model.PredefinedPoint{
// 		Label: addPredefinedPoint.Label,
// 		Name: addPredefinedPoint.Name,
// 		Database: addPredefinedPoint.Database,
// 		Measurement: addPredefinedPoint.Measurement,
// 	}
// 	lastID, err := account.Insert()
// 	if err != nil {
// 		httputil.NewError(ctx, http.StatusBadRequest, err)
// 		return
// 	}
// 	account.ID = lastID
// 	ctx.JSON(http.StatusOK, account)
// }