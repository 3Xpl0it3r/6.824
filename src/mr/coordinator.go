package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Stage string

const (
	MapTaskStage    Stage = "map"
	ReduceTaskStage Stage = "reduce"
)

const defaultTTL = 5 * time.Second

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int

	taskMtx               sync.Mutex // 目前都是单个用户线程，理论上可以不用加锁
	taskQueue             []*Task    // 任务队列
	intermediateLocations []string   // 中间文件列表
	taskCh                chan *Task // 用来传递任务

	// coordinate 状态
	stageMtx sync.Mutex
	Stage    Stage

	// 控制coordinator 退出
	allDone chan struct{}

}
func (c *Coordinator) main() {
	defer func() {
		close(c.taskCh)
		close(c.allDone)
	}()
Finish:
	for true {
		select {
		default:
			c.healthCheck()
		}
		switch c.getStage() {
		case MapTaskStage:
			if c.isAllFinished(MapTaskStage) {
				c.switchStage(MapTaskStage, ReduceTaskStage)
			}
			if task, ok := c.getTask(); ok {
				c.taskCh <- task
			}
		case ReduceTaskStage:
			if c.isAllFinished(ReduceTaskStage) {
				time.Sleep(1 * time.Second)
				break Finish
			}
			if task, ok := c.getTask(); ok {
				c.taskCh <- task
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func (c *Coordinator) initCoordinator(files []string, nReduce int) {
	c.files = files
	c.nReduce = nReduce
	// init some fields about map
	c.taskMtx = sync.Mutex{}
	c.initMapperTask()
	c.intermediateLocations = make([]string, 0)
	c.taskCh = make(chan *Task)
	// init some fields about stage
	c.stageMtx = sync.Mutex{}
	c.Stage = MapTaskStage
	//
	c.allDone = make(chan struct{})
}

// 初始化map task
func (c *Coordinator) initMapperTask() {
	c.taskMtx.Lock()
	defer c.taskMtx.Unlock()
	c.taskQueue = make([]*Task, 0)
	for idx := 0; idx < len(c.files); idx++ {
		c.taskQueue = append(c.taskQueue, &Task{
			Id:            TaskId(idx),
			Kind: MapTask,
			Inputs:        c.files[idx],
			NReduce:       c.nReduce,
			Intermediates: nil,
			Status:        UnAssigned,
			CreateAt:       time.Now(),
		})
	}
}

// 初始化reduce task
func (c *Coordinator) initReducerTask() {
	c.taskMtx.Lock()
	defer c.taskMtx.Unlock()
	c.taskQueue = c.taskQueue[:0]
	for idx := 0; idx < c.nReduce; idx++ {
		c.taskQueue = append(c.taskQueue, &Task{
			Id:            TaskId(idx),
			Inputs:        "",
			NReduce:       c.nReduce,
			Intermediates: c.intermediateLocations,
			Status:        UnAssigned,
			Kind: ReduceTask,
			CreateAt:       time.Now(),
		})
	}
}

// 任务是否完成
func (c *Coordinator) isAllFinished(stage Stage) bool {
	c.taskMtx.Lock()
	defer c.taskMtx.Unlock()
	allFinished := 0
	for _, task := range c.taskQueue {
		if task.Status == Finished {
			allFinished += 1
		}
	}
	if stage == MapTaskStage {
		return allFinished == len(c.files)
	}
	return allFinished == c.nReduce
}

// 调度task
func (c *Coordinator) getTask() (*Task, bool) {
	c.taskMtx.Lock()
	defer c.taskMtx.Unlock()
	for _, t := range c.taskQueue {
		if t.Status == UnAssigned {
			t.Status = Assigned
			t.CreateAt = time.Now()
			return t, true
		}
	}
	return nil, false
}

// 更新任务列表
func (c *Coordinator) updateTask(task *Task) bool {
	c.taskMtx.Lock()
	defer c.taskMtx.Unlock()
	for _, t := range c.taskQueue {
		if t.Id == task.Id {
			if t.Status == Finished{
				return true
			} else {
				t.Status = task.Status
			}
			break
		}
	}

	if task.Status == Finished && task.Kind == MapTask {
		c.intermediateLocations = append(c.intermediateLocations, task.Intermediates...)
	}
	return false
}

// 获取当前stage
func (c *Coordinator) getStage() Stage {
	c.stageMtx.Lock()
	defer c.stageMtx.Unlock()
	return c.Stage
}

//  状态切换
func (c *Coordinator) switchStage(from, to Stage) {
	c.stageMtx.Lock()
	defer c.stageMtx.Unlock()
	if from == to {
		return
	}
	if from == MapTaskStage && to == ReduceTaskStage {
		c.Stage = to
		c.initReducerTask()
	}
}

// 健康检查
func (c *Coordinator) healthCheck() {
	c.taskMtx.Lock()
	defer c.taskMtx.Unlock()
	for _, task := range c.taskQueue {
		if task.Status == Finished{
			continue
		}
		if time.Now().Sub(task.CreateAt) > defaultTTL {
			task.Status = UnAssigned
		}
	}
}


// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskAssign(args *TaskRequestArgs, reply *TaskRequestReply) error {
	//fmt.Fprintf(os.Stdout, "[%s] require worker......\n", args.WorkerId)
	task := <-c.taskCh


	reply.AllComplete = false
	if task == nil {
		reply.AllComplete = true
		return nil
	}
	reply.Id = task.Id
	reply.Kind = task.Kind
	reply.Intermediates = task.Intermediates
	reply.Inputs = task.Inputs
	reply.NumReducer = task.NReduce
	// fmt.Fprintf(os.Stdout, "-->[%s] get taskCh Id: [%d]  Kind: [%s]\n", args.WorkerId, task.Id, task.Kind)
	return nil
}

func (c *Coordinator) TaskReport(args *TaskReportArgs, reply *TaskReportReply) error {
	var task Task
	task.Kind = args.Kind
	if args.Status == Failed {
		task.Status = UnAssigned
	} else {
		task.Status = Finished
	}
	task.Id = args.Id
	task.Intermediates = args.Intermediates
	reply.NeedDrop = c.updateTask(&task)
	if reply.NeedDrop && args.Kind == ReduceTask{
		os.Remove(args.MergeFile)
	}
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
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
	ret := false

	// Your code here.


	return ret
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
