package mr

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Stage string

const (
	MapTaskStage    Stage = "map"
	ReduceTaskStage Stage = "reduce"
)

type Coordinator struct {

	// Your definitions here.
	// the type of worker

	files   []string
	nReduce int
	nMap    int // not used

	taskMtx             sync.Mutex
	taskUnAssignedQueue []Task
	taskProcessingQueue []Task
	taskFinishedQueue   []Task
	sequence            int // 用来记录任务seq ，当任务已经全部分配出去，此时有新worker进来，则将最早的task分配

	allDone  chan struct{}
	assignCh chan Task

	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once

	stageMtx sync.Mutex
	Stage    Stage
}

// 初始化mapper task 列表
func (c *Coordinator) initCoordinator(files []string, nReduce int) {
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.taskUnAssignedQueue = make([]Task, 0)
	c.nReduce = nReduce
	c.nMap = len(files)
	c.allDone = make(chan struct{})
	c.assignCh = make(chan Task)
	c.sequence = 1
	c.Stage = MapTaskStage
	for i, f := range files {
		c.taskUnAssignedQueue = append(c.taskUnAssignedQueue,
			Task{Id: TaskId(i), Inputs: f, NumReduce: c.nReduce, Kind: MapTask, Sequence: c.sequence})
		c.sequence += 1
	}
}

// 初始化reducer task 列表
func (c *Coordinator) initReduceTask() {
	c.taskMtx.Lock()
	defer c.taskMtx.Unlock()
	c.taskUnAssignedQueue = c.taskUnAssignedQueue[:0]
	c.taskFinishedQueue = c.taskFinishedQueue[:0]
	c.taskProcessingQueue = c.taskProcessingQueue[:0]
	intermediates := []string{}
	for i := 0; i < c.nMap; i++ {
		intermediates = append(intermediates, strconv.Itoa(i))
	}

	for i := 0; i < c.nReduce; i++ {
		c.taskUnAssignedQueue = append(c.taskUnAssignedQueue, Task{
			Id:            TaskId(i),
			Kind:          ReduceTask,
			Inputs:        "",
			NumReduce:     c.nMap,
			Intermediates: intermediates,
		})
	}
}

func (c *Coordinator) main() {
	defer func() {
		close(c.assignCh)
		close(c.allDone)
	}()
LOOP:
	for true {
		stage := c.getStage()
		switch stage {
		case MapTaskStage:
			if c.taskIsComplete() {
				c.updateStage(ReduceTaskStage)
				c.initReduceTask()
				continue
			}
			task, _ := c.getTask(c.nMap)
			c.assignCh <- task
		case ReduceTaskStage:
			if c.taskIsComplete() {
				break LOOP
			}
			task, _ := c.getTask(c.nReduce)
			c.assignCh <- task
		}
		time.Sleep(1 * time.Second)
	}
}

func (c *Coordinator) taskIsComplete() bool {
	c.taskMtx.Lock()
	defer c.taskMtx.Unlock()
	stage := c.getStage()
	if stage == MapTaskStage {
		return len(c.taskFinishedQueue) == c.nMap
	} else if stage == ReduceTaskStage {
		return len(c.taskFinishedQueue) == c.nReduce
	} else {
		return false
	}
}

// getTask get task
// 如果unAssigned队列不为空，则从unAssigned里面直接返回一个seq最小的
// 如果unAssigned队列为空&proces队列不为空，此时仍然有worker进来请求，获取seq最小的那个，
func (c *Coordinator) getTask(totalTask int) (task Task,ok bool) {
	c.taskMtx.Lock()
	defer c.taskMtx.Unlock()
	if len(c.taskFinishedQueue) == totalTask {
		return Task{}, false
	}
	ok = true
	// taskUnAssignedQueue is not empty
	if len(c.taskUnAssignedQueue) != 0 {
		task = c.taskUnAssignedQueue[0]
		c.taskUnAssignedQueue = append(c.taskUnAssignedQueue[:0], c.taskUnAssignedQueue[1:]...)
		c.taskProcessingQueue = append(c.taskProcessingQueue, task)
		return
	}

	if len(c.taskProcessingQueue) == 0{
		ok = false
		return Task{}, false
	}
	// taskUnAssignedQueue is empty,but taskProcessing is not empty
	// pick up the oldest task from taskProcessing , we assume the worker that processing this task is dead
	task = c.taskProcessingQueue[0]
	task.Sequence += 1
	c.taskProcessingQueue = append(c.taskProcessingQueue[:0], c.taskProcessingQueue[1:]...)
	c.taskProcessingQueue = append(c.taskProcessingQueue, task)
	return

}

// updateTaskList
// if task has complete then put it to taskSuccessfully list
// if task failed then put it re back to taskUnAssigned list
func (c *Coordinator) updateTaskList(id TaskId, complete bool) {
	c.taskMtx.Lock()
	defer c.taskMtx.Unlock()
	var task Task
	for i, t := range c.taskProcessingQueue {
		if t.Id == id && t.Sequence == t.Sequence {
			task = c.taskProcessingQueue[i]
			c.taskProcessingQueue = append(c.taskProcessingQueue[:i], c.taskProcessingQueue[i+1:]...)
			break
		}
	}
	if complete {
		c.taskFinishedQueue = append(c.taskFinishedQueue, task)
	} else {
		c.taskUnAssignedQueue = append(c.taskUnAssignedQueue, task)
	}
}

func (c *Coordinator) getStage() Stage {
	c.stageMtx.Lock()
	defer c.stageMtx.Unlock()
	return c.Stage
}

func (c *Coordinator) updateStage(stage Stage) {
	c.stageMtx.Lock()
	defer c.stageMtx.Unlock()
	c.Stage = stage
}

// Your code here -- RPC handlers for the worker to call.
// TaskScheduler is used for assign task for worker
func (c *Coordinator) TaskAssign(args *TaskAssignArgs, reply *TaskAssignReply) error {
	task, ok := <-c.assignCh
	if !ok {
		reply.Status = false
		return nil
	}
	reply.Targets = task.Intermediates
	reply.Status = true
	reply.NumReducer = c.nReduce
	reply.Kind = task.Kind
	reply.Id = task.Id
	reply.Inputs = task.Inputs
	return nil
}

func (c *Coordinator) TaskReport(args *TaskReportArgs, reply *TaskReportReply) error {
	var complete bool = args.Status == TaskSuccess
	c.updateTaskList(args.Id, complete)
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	fmt.Fprintf(os.Stdout, "rpc called")
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	defer func() {
		c.cancel()
	}()
	<-c.allDone
	// Your code here.
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.initCoordinator(files, nReduce)
	go c.main()

	c.server()
	return &c
}
