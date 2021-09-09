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


type TaskReportArgs struct {
	Kind TaskKind							// Kind represent the kind of task
	Id TaskId
	Status TaskStatus
	Err string
}
type TaskReportReply struct {
	// none
}

// used to request task from coordinator
type TaskAssignArgs struct {
	// in real word may works has some meta data, like ip address or node name or ...
	// but in this lab is None, some meta data is assigned by coordinator
	Id TaskId
}
//
type TaskAssignReply struct {
	Kind TaskKind
	Id TaskId
	Status bool

	// special for map task
	Inputs string 			// 要处理的文件
	NumReducer int


	// special for reduce task
	Targets []string 			// 处理完后的文件的location， 在这个lab里面就指代所有的workers, 所有的文件路径格式都是固定的
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
