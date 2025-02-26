package main

import (
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const ABS_FILE_PATH string = "C:\\Users\\jaypr\\Desktop\\GO\\prep\\distsys\\Map Reduce\\RPCDist\\files\\"

var WORKER_ID int

var WORKER_ADDRESS = "localhost:8800"

func main() {
	args := os.Args[1:]
	address := args[0]
	WORKER_ADDRESS = address
	// start the Coordinator Server (parse the input)
	// run the workers in goroutines
	// barrier until the tasks has been completed
	client, err := rpc.Dial("tcp", "localhost:8880")
	if err != nil {

		fmt.Println("Could not process query", err)
		return
	}

	// the server should have a ID
	err = client.Call("CoordinationServer.Register", WORKER_ADDRESS, &WORKER_ID)
	if err != nil {
		fmt.Println("Process request ", err)
		return
	}

	go runHealthCheck(client)

	coordinator := CoordinatorServer{
		Address:      WORKER_ADDRESS,
		JobStatus:    make(map[string]string),
		mu:           sync.Mutex{},
		ReduceStatus: make(map[string]string),
	}
	fmt.Printf("The Coordinator is starting in address %v\n", coordinator.Address)

	for {
		time.Sleep(5 * time.Second)
		var jobs []Job
		fmt.Println("Requesting Jobs")
		err = client.Call("CoordinationServer.RequestJobs", WORKER_ADDRESS, &jobs)
		if err != nil {
			fmt.Println("Process request ", err)
			return
		}
		if len(jobs) == 0 {
			continue
		}
		for _, element := range jobs {
			absFilePath := filepath.Join(ABS_FILE_PATH, element.Filename)
			coordinator.JobStatus[absFilePath] = JOB_IDLE
		}

		coordinator.Listen()

		intermediateFiles := make(chan string, 10)
		go WorkerMap(1, coordinator.Address, intermediateFiles)
		go WorkerMap(2, coordinator.Address, intermediateFiles)
		go WorkerMap(3, coordinator.Address, intermediateFiles)

		fmt.Println("The workers have started")
		<-coordinator.CheckMapPhaseOver()
		fmt.Println("The Map Phase is over")
		close(intermediateFiles)
		for key, value := range coordinator.JobStatus {
			fmt.Printf("Key: %v and Value: %v\n", key, value)
		}
		// Work completed, check intermediate files generated for output
		// aba reduction wala process suru garnu paryo
		// also worker ma 10 seconds ma bhayena bhane fail wala code
		// need to update the coordinator server struct for more information
		// assume 3 workers as of now

		for element := range intermediateFiles {
			coordinator.ReduceStatus[element] = JOB_IDLE
			fmt.Println(element)
		}
		fmt.Println("Starting Reduction Phase")
		go WorkerReduce(0, coordinator.Address)
		go WorkerReduce(1, coordinator.Address)
		go WorkerReduce(2, coordinator.Address)
		// first worker will pick up job for-all <file-number><0>.txt files from the map part

		<-coordinator.CheckReducePhaseOver()

		fmt.Println("THE MAP REDUCE TASK HAS BEEN COMPLETED")

		// since there are only 3 worker IDs, we can create one single reduction file
		// cleanup the intermediate files produced
		// coordinator.Cleanup()
	}

}
