package scheduler

import "sync"

type Job struct {
	Uid      int16
	Name     string
	Tasks    map[string]*Task
	Schedule string
	Dag      dag
	Active   bool
	JobState *jobState
}

type jobState struct {
	sync.RWMutex
	State     state           `json:"state"`
	TaskState *stringStateMap `json:"taskState"`
}

type stringStateMap struct {
	sync.RWMutex
	Internal map[string]state `json:"internal"`
}

type state string

type Task struct {
	Name              string
	Operator          Operator
	TriggerRule       triggerRule
	Retries           int
	RetryDelay        RetryDelay
	AttemptsRemaining int
}
type triggerRule string

type Operator interface {
	Run() (interface{}, error)
}

type RetryDelay interface {
	wait(taskName string, attempt int)
}
