package mapreduce

import (
	"fmt"
	//"strconv"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	// Register worker(s)
	switch phase {
	case mapPhase:
		for fileIndex, fileName := range mr.files {
				// Schedule work to be done:
				mapArgs := new(DoTaskArgs)
				mapArgs.JobName = mr.jobName
				mapArgs.File = fileName
				mapArgs.Phase = phase
				mapArgs.TaskNumber = fileIndex
				mapArgs.NumOtherPhase = nios
				go func() {
					workerName := <- mr.registerChannel
					fmt.Println("MAP " + workerName)
					call(workerName, "Worker.DoTask", mapArgs, new(struct{}))
				}()
		}
	case reducePhase:
		for i := 0; i < ntasks; i++ {
				// Schedule work to be done:
				reduceArgs := new(DoTaskArgs)
				reduceArgs.JobName = mr.jobName
				reduceArgs.Phase = phase
				reduceArgs.TaskNumber = i
				reduceArgs.NumOtherPhase = nios
				//fmt.Println(reduceArgs.JobName)
				go func() {
					workerName := <- mr.registerChannel
					call(workerName, "Worker.DoTask", reduceArgs, new(struct{}))
					mr.registerChannel <- workerName
				}()
		}
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}
