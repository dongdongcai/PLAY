package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	var wg sync.WaitGroup
	for jobNum := 0; jobNum < ntasks; jobNum++ {
		wg.Add(1)
		go func(taskNum int) {
			defer wg.Done()
			worker := <-registerChan
			ok := call(worker, "Worker.DoTask", DoTaskArgs{jobName, mapFiles[taskNum], phase, taskNum, n_other}, nil)
			if ok {
				//Note: Use go routine here since the channel is unbuffered!!!!!
				go func() {
					registerChan <- worker
				}()
			}
		}(jobNum)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
