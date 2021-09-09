package mr


type TaskKind string

const (
	MapTask TaskKind = "map"
	ReduceTask TaskKind = "reduce"
)

// TaskId represent the identifier for task
type TaskId int


// TaskStatus represent the status of task
type TaskStatus int

const (
	TaskSuccess TaskStatus = iota + 1		// task has complete with successfully
	TaskFailed								// task finished with failed
)

// Task represent the task will be assigned to worker
type Task struct {
	Id TaskId
	Kind TaskKind
	Sequence int
	// for map
	Inputs string
	NumReduce int

	// for reduce
	Intermediates []string
}



type WorkerStatus int
const (

)

type MapReduceWorker struct {
}
