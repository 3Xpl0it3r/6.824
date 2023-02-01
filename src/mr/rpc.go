package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// 申请task
type TaskRequestArgs struct {
	WorkerId string
}

//
type TaskRequestReply struct {
	Kind TaskKind
	Id   TaskId

	// 针对map类型task
	Inputs     string // 要处理的文件
	NumReducer int    // reducer 数量

	// special for reduce task
	Intermediates []string // 中间文件路径

	AllComplete bool
}

type TaskReportArgs struct {
	WorkerId string
	Kind   TaskKind // Kind represent the kind of task
	Id     TaskId
	Status TaskStatus
	Err    string
	// for map
	Intermediates []string // 中间文件路径

	//
	MergeFile string

}
type TaskReportReply struct {
	// none
	NeedDrop bool
}



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
