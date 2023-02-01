package mr

import "time"

type TaskKind string

const (
	MapTask    TaskKind = "mapper"
	ReduceTask TaskKind = "reducer"
)

// Task represent the task will be assigned to worker
type Task struct {
	Kind          TaskKind
	Id            TaskId   // the id of task
	Inputs        string   // Inputs is for mapper worker
	NReduce       int      // NumReduce is for mapper worker, immediate will split to NumReduce blocks through hash function
	Intermediates []string // generate by mapper worker
	OutputFile string
	Status        TaskStatus
	CreateAt      time.Time
}

// TaskId represent the identifier for task
type TaskId int

// TaskStatus represent the status of task
type TaskStatus string

const (
	Assigned   TaskStatus = "assigned"
	UnAssigned TaskStatus = "unassigned"
	Finished   TaskStatus = "finished"

	Success    TaskStatus = "success"
	Failed     TaskStatus = "failed"
)
