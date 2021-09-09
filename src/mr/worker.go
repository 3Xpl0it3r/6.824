package mr

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


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
	var err error
	// Your worker implementation here.

	for true {
		t := CallTaskAssign()
		if !t.Status {
			break
		}
		if t.Kind == MapTask{
			err = doMapWorker(&t, mapf)
		} else if  t.Kind == ReduceTask{
			err = doReduceWorker(&t, reducef)
			if err != nil{
			}
		} else {
			err = errors.New("unknown task kind" + string(t.Kind))
		}

		if err != nil{
			fmt.Fprintf(os.Stdout, "----> run worker [%v][%v] error %v\n", t.Kind, t.Id, err)
		}
		// uncomment to send the Example RPC to the coordinator.
		// if task complete report to coordinator

		CallTaskReport(t.Id, t.Kind, err)
	}
}

func workerHashId()string{
	hash := md5.Sum([]byte(string(time.Now().UnixNano())))
	return hex.EncodeToString(hash[:])
}

func doMapWorker(task *mapReduceTask, mapF func(string,string)[]KeyValue)error{
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
		immediateFile := genImmediateFileName(int(task.Id), idx)
		fp,err := ioutil.TempFile("", immediateFile)
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
		if err := os.Rename(fp.Name(), immediateFile);err != nil{
			os.Remove(fp.Name())
			return err
		}
	}
	return nil
}

func doReduceWorker(task *mapReduceTask, reduceF func(string, []string)string)error{
	targets := fetchImmediateFileName(task.Targets, int(task.Id))
	maps := make(map[string][]string)
	for _,target := range targets{
		fp,err := os.OpenFile(target, os.O_RDONLY, 0666)
		if err != nil{
			return err
		}
		decode := json.NewDecoder(fp)
		for {
			var kv KeyValue
			if err := decode.Decode(&kv);err != nil{
				break
			}
			if _, ok := maps[kv.Key];ok {
				maps[kv.Key] = make([]string, 0)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	res := make([]string, 0)
	for k,v := range maps{
		res = append(res, fmt.Sprintf("%v %v\n", k, reduceF(k, v)))
	}
	resultFile := genResultFileName(int(task.Id))
	fp,err := ioutil.TempFile("",resultFile)
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
	if err := os.Rename(fp.Name(), resultFile);err != nil{
		return err
	}
	return nil
}


func genImmediateFileName(taskId, idx int)string{
	return fmt.Sprintf("mr-%d-%d", taskId, idx)
}

func fetchImmediateFileName(targets []string, idx int)[]string{
	results := make([]string, 0)
	for _, t := range targets{
		results = append(results, "mr-"+t+"-"+strconv.Itoa(idx))
	}
	return results
}

func genResultFileName(reduceId int)string{
	return fmt.Sprintf("mr-out-%d", reduceId)
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// mapReduceTask represent the
type mapReduceTask struct {
	Kind TaskKind
	Id TaskId
	NReduce int
	Status bool

	Inputs string
	Targets []string

}

// CallRequestTask request from coordinator
func CallTaskAssign()mapReduceTask{
	t := mapReduceTask{}

	args := TaskAssignArgs{}
	reply := TaskAssignReply{}
	call("Coordinator.TaskAssign", &args, &reply)
	t.Kind = reply.Kind
	t.Id = reply.Id
	t.Inputs = reply.Inputs
	t.Targets = reply.Targets
	t.NReduce = reply.NumReducer
	t.Status = reply.Status
	return t
}

// CallTaskHealthReport report task status to coordinator
func CallTaskReport(taskId TaskId, taskKind TaskKind, err error){
	args := TaskReportArgs{Id: taskId, Kind: taskKind}
	if err != nil{
		args.Status = TaskFailed
		args.Err = err.Error()
	}else {
		args.Status = TaskSuccess
	}
	reply := TaskReportReply{}
	call("Coordinator.TaskReport", &args, &reply)
}



func Health(){
	// todo
}

//
// send an RPC request to the coordinator, wFDait for the response.
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
