package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const ABS_FILE_PATH string = "C:\\Users\\jaypr\\Desktop\\GO\\prep\\distsys\\Map Reduce\\RPCDist\\"

func main() {
	args := os.Args[1:]
	// start the Coordinator Server (parse the input)
	// run the workers in goroutines
	// barrier until the tasks has been completed
	coordinator := CoordinatorServer{
		Address:      "localhost:8888",
		JobStatus:    make(map[string]string),
		mu:           sync.Mutex{},
		ReduceStatus: make(map[string]string),
	}
	fmt.Printf("The Coordinator is starting in address %v\n", coordinator.Address)
	for _, element := range args {
		absFilePath := filepath.Join(ABS_FILE_PATH, element)
		coordinator.JobStatus[absFilePath] = JOB_IDLE
	}
	coordinator.Listen()

	go WorkerMap(1, coordinator.Address)
	go WorkerMap(2, coordinator.Address)
	go WorkerMap(3, coordinator.Address)

	fmt.Println("The workers have started")
	<-coordinator.CheckMapPhaseOver()
	fmt.Println("The Map Phase is over")
	for key, value := range coordinator.JobStatus {
		fmt.Printf("Key: %v and Value: %v\n", key, value)
	}
	// Work completed, check intermediate files generated for output
	// aba reduction wala process suru garnu paryo
	// also worker ma 10 seconds ma bhayena bhane fail wala code
	// need to update the coordinator server struct for more information
	// assume 3 workers as of now

	for i := range 3 {
		for j := range 3 {
			coordinator.ReduceStatus[fmt.Sprintf("file-%v-%v", j+1, i)] = JOB_IDLE
		}
	}

	go WorkerReduce(0, coordinator.Address)
	go WorkerReduce(1, coordinator.Address)
	go WorkerReduce(2, coordinator.Address)
	// first worker will pick up job for-all <file-number><0>.txt files from the map part

	<-coordinator.CheckReducePhaseOver()

	fmt.Println("THE MAP REDUCE TASK HAS BEEN COMPLETED")

	// since there are only 3 worker IDs, we can create one single reduction file
	// cleanup the intermediate files produced
	coordinator.Cleanup()

}
