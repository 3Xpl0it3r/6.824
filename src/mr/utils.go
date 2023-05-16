package mr

import (
	"fmt"
	"os"
	"strconv"
)

// immediateFilename generate immediateFile， it produced by mapper worker
// in this lab ,it location local filesystem
func intermediateFilePrefix(taskId TaskId,workerId string)string{
	return fmt.Sprintf("mr-%d-%s", int(taskId), workerId)
}

func intermediateFileName(taskId TaskId, workerId string, idx int)string{
	return fmt.Sprintf("mr-%d-%s-%d", int(taskId),workerId, idx)
}



// immediateFileLocation will be used by reducer in reduce stage
func intermediateLocationList(task *Task)[]string{
	locations := make([]string, 0)
	for _, t := range task.Intermediates{
		locations = append(locations, t + "-" + strconv.Itoa(int(task.Id)))
	}
	return locations
}

// resultOutputFilename is produced by reducer worker
func mergedFile(reduceId int,workerId string)string{
	return fmt.Sprintf("mr-out-%d-%s", reduceId,workerId)
}

func resultFile(reducedId int)string{
	return fmt.Sprintf("mr-out-%d", reducedId)
}


func getWorkerIAddress()string{
	return strconv.Itoa(os.Getpid())
}
