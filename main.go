package main

import (
	"errors"
	"fmt"
	"go-api-grapqhl/controller"
	_ "go-api-grapqhl/docs"
	"go-api-grapqhl/graph"
	"go-api-grapqhl/scheduler"

	// "go-api-grapqhl/graph/client"
	"go-api-grapqhl/graph/client"
	"go-api-grapqhl/graph/generated"
	"go-api-grapqhl/httputil"
	"net/http"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/gin-gonic/gin"
	"github.com/robfig/cron/v3"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// @title           Swagger Example API
// @version         1.0
// @description     This is a sample server celler server.
// @termsOfService  http://swagger.io/terms/

// @contact.name   API Support
// @contact.url    http://www.swagger.io/support
// @contact.email  support@swagger.io

// @license.name  Apache 2.0
// @license.url   http://www.apache.org/licenses/LICENSE-2.0.html

// @host      localhost:8080
// @BasePath  /api/v1

// @securityDefinitions.basic  BasicAuth

// @securityDefinitions.apikey  ApiKeyAuth
// @in                          header
// @name                        Authorization
// @description					Description for what is this security definition being used

// @securitydefinitions.oauth2.application  OAuth2Application
// @tokenUrl                                https://example.com/oauth/token
// @scope.write                             Grants write access
// @scope.admin                             Grants read and write access to administrative information

// @securitydefinitions.oauth2.implicit  OAuth2Implicit
// @authorizationUrl                     https://example.com/oauth/authorize
// @scope.write                          Grants write access
// @scope.admin                          Grants read and write access to administrative information

// @securitydefinitions.oauth2.password  OAuth2Password
// @tokenUrl                             https://example.com/oauth/token
// @scope.read                           Grants read access
// @scope.write                          Grants write access
// @scope.admin                          Grants read and write access to administrative information

// @securitydefinitions.oauth2.accessCode  OAuth2AccessCode
// @tokenUrl                               https://example.com/oauth/token
// @authorizationUrl                       https://example.com/oauth/authorize
// @scope.admin                            Grants read and write access to administrative information

func graphqlHandler() gin.HandlerFunc {
    // NewExecutableSchema and Config are in the generated.go file
    // Resolver is in the resolver.go file    
	h := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{Resolvers: &graph.Resolver{}}))

    return func(c *gin.Context) {
        h.ServeHTTP(c.Writer, c.Request)
    }
}

// Defining the Playground handler
func playgroundHandler() gin.HandlerFunc {
    h := playground.Handler("GraphQL", "/query")

    return func(c *gin.Context) {
        h.ServeHTTP(c.Writer, c.Request)
	}
}


func main() {

	fmt.Println("Start API server!!!!!")

	client.AddClientPoint("neo4j://localhost:7687", "neo4j", "test", 
	"WIIOT", "Utility_3", false, client.TaggingPoint{
		BMS_id: "UT3_CH01_Indi_Flow",
		PointName: "UT3_CH01_Indi_Flow",
		System: "HVAC_System",
		SubSystem: "Water_System",
		DeviceType: "Chiller",
		DeviceName: "CH01",
		PointType: "Chiller_Water_Flowrate",
		Location: "Building",
		Level: "UT3",
		ClassType: "Class",
		Interval: "20T",
		Unit: "°C",

	}, []string{"bldg"}...)
	r := gin.Default()

	cr := cron.New()
	cr.Start()
	cr.AddFunc("*/5 * * * *", func() {scheduler.Analytics()})
	cr.AddFunc("*/5 * * * *", func() {scheduler.IndividualAnalytics()})


	r.POST("/query", graphqlHandler())
    r.GET("/", playgroundHandler())

	c := controller.NewController()

	v1 := r.Group("/api/v1")
	{
		accounts := v1.Group("/accounts")
		{
			accounts.GET(":id", c.ShowAccount)
			accounts.GET("", c.ListAccounts)
			accounts.POST("", c.AddAccount)
			accounts.DELETE(":id", c.DeleteAccount)
			accounts.PATCH(":id", c.UpdateAccount)
			accounts.POST(":id/images", c.UploadAccountImage)
		}
		bottles := v1.Group("/bottles")
		{
			bottles.GET(":id", c.ShowBottle)
			bottles.GET("", c.ListBottles)
		}
		admin := v1.Group("/admin")
		{
			admin.Use(auth())
			admin.POST("/auth", c.Auth)
		}
		examples := v1.Group("/examples")
		{
			examples.GET("ping", c.PingExample)
			examples.GET("calc", c.CalcExample)
			examples.GET("groups/:group_id/accounts/:account_id", c.PathParamsExample)
			examples.GET("header", c.HeaderExample)
			examples.GET("securities", c.SecuritiesExample)
			examples.GET("attribute", c.AttributeExample)
		}
		neo4j := v1.Group("/neo4j")
		{
			neo4j.GET("/findnode/:database/:measurement/:label/:name", c.ShowNodeByName)
			neo4j.GET("/findadjnodes/:database/:measurement/:label/:name", c.ShowAdjNodesByName)
		}
		influxdb := v1.Group("/influxdb")
		{
			influxdb.POST("/query", c.QueryInfluxDB)
		}
	}
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	r.Run(":8080")
}

func auth() gin.HandlerFunc {
	return func(c *gin.Context) {
		if len(c.GetHeader("Authorization")) == 0 {
			httputil.NewError(c, http.StatusUnauthorized, errors.New(" Authorization is required Header"))
			c.Abort()
		}
		c.Next()
	}
}
