package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
    var err error
	workerAddress := getWorkerIAddress()

	for true {
		t, complete := CallTaskAssign(workerAddress)
		if complete {
			return
		}

		if t.Kind == MapTask {
			err = doMapWorker(workerAddress, &t, mapf)
		} else if t.Kind == ReduceTask {
			err = doReduceWorker(workerAddress, &t, reducef)
			if err != nil {
			}
		} else {
			err = errors.New("unknown task kind" + string(t.Kind))
		}

		if err != nil {
			fmt.Fprintf(os.Stdout, "----> run worker [%v][%v] error %v\n", t.Kind, t.Id, err)
		}
		// uncomment to send the Example RPC to the coordinator.
		// if task complete report to coordinator
		CallTaskReport(workerAddress, &t, err)
	}
}

func doMapWorker(workerId string,task *Task, mapF func(string,string)[]KeyValue)error{
	fp,err := ioutil.ReadFile(task.Inputs)
	if err != nil{
		return err
	}
	kvs := mapF(task.Inputs, string(fp))
  	immediate := make([][]KeyValue, task.NReduce)
  	for _, kv := range kvs{
  		hash := ihash(kv.Key) % task.NReduce
  		immediate[hash] = append(immediate[hash], kv)
	}
	for idx, kvs := range immediate{
		intermediaFile := intermediateFileName(task.Id, workerId,idx)
		fp,err := ioutil.TempFile("", intermediaFile)
		if err != nil{
			return err
		}
		enc := json.NewEncoder(fp)
		for _,kv := range kvs{
			if err := enc.Encode(kv);err != nil{
				return err
			}
		}
		if err := fp.Close();err != nil{
			return err
		}
		if err := os.Rename(fp.Name(), intermediaFile);err != nil{
			os.Remove(fp.Name())
			return err
		}
	}
	task.Intermediates = []string{intermediateFilePrefix(task.Id, workerId)}
	return nil
}

func doReduceWorker(workerId string,task *Task, reduceF func(string, []string)string)error{
	targetLocations := intermediateLocationList(task)
	maps := make(map[string][]string)
	for _,target := range targetLocations{
		fp,err := os.OpenFile(target, os.O_RDONLY, 0666)
		if err != nil{
			return err
		}
		decode := json.NewDecoder(fp)
		for decode.More(){
			var kv KeyValue
			if err := decode.Decode(&kv);err != nil{
				break
			}

			if _, ok := maps[kv.Key];!ok {
				maps[kv.Key] = make([]string, 0)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	res := make([]string, 0)
	for k,v := range maps{
		res = append(res, fmt.Sprintf("%v %v\n", k, reduceF(k, v)))
	}
	mergedFile := mergedFile(int(task.Id), workerId)
	fp,err := ioutil.TempFile("",mergedFile)
	defer fp.Close()
	if err != nil{
		return err
	}
	for _,re := range res{
		if _,err = fp.WriteString(re);err != nil{
			_ = os.Remove(fp.Name())
			return err
		}
	}
	if err := os.Rename(fp.Name(), mergedFile);err != nil{
		return err
	}
	task.OutputFile = mergedFile
	return nil
}


//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// CallRequestTask request from coordinator
func CallTaskAssign(workerId string) (task Task, allComplete bool) {
	allComplete = false
	args := TaskRequestArgs{
		WorkerId: workerId,
	}
	reply := TaskRequestReply{}
	call("Coordinator.TaskAssign", &args, &reply)
	if reply.AllComplete {
		return Task{}, true
	}
	task.Kind = reply.Kind
	task.Id = reply.Id
	task.Inputs = reply.Inputs
	task.Intermediates = reply.Intermediates
	task.NReduce = reply.NumReducer
	return
}

// CallTaskHealthReport report task status to coordinator
func CallTaskReport(workerId string, task *Task, err error) {
	args := TaskReportArgs{Id: task.Id, Kind: task.Kind, WorkerId: workerId, Intermediates: task.Intermediates, MergeFile: task.OutputFile}
	if err != nil {
		args.Status = Failed
		args.Err = err.Error()
	} else {
		args.Status = Success
	}
	reply := TaskReportReply{}
	call("Coordinator.TaskReport", &args, &reply)
	//if reply.NeedDrop && task.Kind == ReduceTask{
	//	os.Rename(mergedFile(int(task.Id), workerId), resultFile(int(task.Id)))
	//}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
