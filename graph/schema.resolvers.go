package graph

// This file will be automatically regenerated based on the schema, any resolver implementations
// will be copied through when generating and any unknown code will be moved to the end.

import (
	"context"
	"encoding/json"
	"fmt"
	"go-api-grapqhl/graph/client"
	"go-api-grapqhl/graph/generated"
	"go-api-grapqhl/graph/model"
	q "go-api-grapqhl/graph/query"
	"go-api-grapqhl/graph/tool"
	"math/rand"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// CreateTodo is the resolver for the createTodo field.
func (r *mutationResolver) CreateTodo(ctx context.Context, input model.NewTodo) (*model.Todo, error) {
	todo := &model.Todo{
		Text:   input.Text,
		ID:     fmt.Sprintf("T%d", rand.Int()),
		User:   &model.User{ID: input.UserID, Name: "user " + input.UserID},
		UserID: input.UserID,
	}
	r.todos = append(r.todos, todo)
	return todo, nil
}

// Todos is the resolver for the todos field.
func (r *queryResolver) Todos(ctx context.Context) ([]*model.Todo, error) {
	return r.todos, nil
}

// Allsys is the resolver for the allsys field.
func (r *queryResolver) Allsys(ctx context.Context, host string, port int, database string, measurement string, energy *bool) (*string, error) {
	dbUri := fmt.Sprintf("neo4j://%s:%d", host, port)
	driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth("neo4j", "test", ""))
	if err != nil {
		panic(err)
	}
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()
	defer driver.Close()
	query := ""
	if energy != nil {
		query = q.QueryAllsysEnergy
	} else {
		query = q.QueryAllsys
	}
	result, err := session.ReadTransaction(client.Query(query, map[string]interface{}{
		"database":    database,
		"measurement": measurement,
	}))
	fmt.Println(result)
	if err != nil {
		panic(err)
	}
	jsonStr, err := json.Marshal(tool.GenerateGraph(result.([][]string)))
	ss := string(jsonStr)
	return &ss, err
}

// Alllocbysys is the resolver for the alllocbysys field.
func (r *queryResolver) Alllocbysys(ctx context.Context, host string, port int, database string, measurement string, system string, energy *bool) (*string, error) {
	dbUri := fmt.Sprintf("neo4j://%s:%d", host, port)
	// dbUri := "neo4j://192.168.100.214:27687"
	driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth("neo4j", "test", ""))
	if err != nil {
		panic(err)
	}
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()
	defer driver.Close()

	query := ""
	if energy != nil {
		query = q.QueryAlllocbysysEnergy
	} else {
		query = q.QueryAlllocbysys
	}
	result, err := session.ReadTransaction(client.Query(query, map[string]interface{}{
		"database":    database,
		"measurement": measurement,
		"system":      system,
	}))
	fmt.Println(result)
	if err != nil {
		panic(err)
	}
	jsonStr, err := json.Marshal(tool.GenerateGraph(result.([][]string)))
	ss := string(jsonStr)
	return &ss, err
}

// Allequipbysysloc is the resolver for the allequipbysysloc field.
func (r *queryResolver) Allequipbysysloc(ctx context.Context, host string, port int, database string, measurement string, system string, location string, energy *bool) ([]*model.Labelvaluepair, error) {
	dbUri := fmt.Sprintf("neo4j://%s:%d", host, port)
	driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth("neo4j", "test", ""))
	if err != nil {
		panic(err)
	}
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()
	defer driver.Close()
	query := ""
	if energy != nil {
		query = q.QueryAllequipbysyslocEnergy
	} else {
		query = q.QueryAllequipbysysloc
	}
	result, err := session.ReadTransaction(client.QueryLabel(query, map[string]interface{}{
		"database":    database,
		"measurement": measurement,
		"system":      system,
		"location":    location,
	}))
	fmt.Println(result)
	if err != nil {
		panic(err)
	}
	res := result.([]string)
	ss := make([]*model.Labelvaluepair, 0)
	for _, ele := range res {
		d := model.Labelvaluepair{Value: ele, Label: ele}
		ss = append(ss, &d)
	}
	return ss, nil
}

// Allparambyequip is the resolver for the allparambyequip field.
func (r *queryResolver) Allparambyequip(ctx context.Context, host string, port int, database string, measurement string, equips string, energy *bool) ([]*model.Labelvaluepair, error) {
	dbUri := fmt.Sprintf("neo4j://%s:%d", host, port)
	driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth("neo4j", "test", ""))
	if err != nil {
		panic(err)
	}
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()
	defer driver.Close()
	query := ""
	if energy != nil {
		query = q.QueryAllparambyequipEnergy
	} else {
		query = q.QueryAllparambyequip
	}
	result, err := session.ReadTransaction(client.QueryLabelValue(query, map[string]interface{}{
		"database":    database,
		"measurement": measurement,
		"equips":      equips,
	}))
	fmt.Println(result)
	if err != nil {
		panic(err)
	}
	res := result.([][]string)
	ss := make([]*model.Labelvaluepair, 0)
	for _, ele := range res {
		d := model.Labelvaluepair{Value: ele[1], Label: ele[0]}
		ss = append(ss, &d)
	}
	return ss, nil
}

// Timeseriesbyid is the resolver for the timeseriesbyid field.
func (r *queryResolver) Timeseriesbyid(ctx context.Context, aggrnum *int, limit *int, startTime *string, endTime *string, database string, measurement string, pointName string, aggreTpye model.AggregationsType) ([]*model.Timeseries, error) {
	query := ""
	if startTime == nil && endTime == nil {
		start := time.Now().Add(-33 * time.Hour).Format("2006-01-02T15:04:05Z")
		end := time.Now().Format("2006-01-02T15:04:05Z")

		if limit != nil {
			query = fmt.Sprintf("SELECT * FROM %s WHERE \"id\"='%s' and (time>'%s' and time<'%s') LIMIT %d ", measurement, pointName, start, end, limit)
		}

	}
	fmt.Print(query)

	ss := make([]*model.Timeseries, 0)
	return ss, nil
}

// Mutation returns generated.MutationResolver implementation.
func (r *Resolver) Mutation() generated.MutationResolver { return &mutationResolver{r} }

// Query returns generated.QueryResolver implementation.
func (r *Resolver) Query() generated.QueryResolver { return &queryResolver{r} }

type mutationResolver struct{ *Resolver }
type queryResolver struct{ *Resolver }
