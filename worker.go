package main

import (
	"errors"
	"fmt"
	"net/rpc"
)

var DIAL_ERROR = errors.New("COULD NOT DIAL SERVER")

func WorkerReduce(workerId int, serverAddress string) {

	client, err := rpc.Dial("tcp", serverAddress)
	if err != nil {
		fmt.Println("Could not dial server")
		return
	}
	reducedMap := make(map[string]int)
	for {
		jobRequestArgs := workerId
		jobRequestReply := ReduceJob{}
		err = client.Call("CoordinatorServer.ReduceRequest", jobRequestArgs, &jobRequestReply)

		if err != nil {
			fmt.Println(err)
			break
		}
		var result = false
		if len(jobRequestReply.Filenames) > 0 {
			fmt.Printf("The job is on %v\n", jobRequestReply.Filenames)
			result = Reduce(jobRequestReply.Filenames, reducedMap, workerId)
		}
		if result {
			for _, element := range jobRequestReply.Filenames {
				fmt.Printf("Updating status of %v\n", element)
				var reply int
				err = client.Call("CoordinatorServer.UpdateReduceStatus", JobRequest{workerId, element, JOB_COMPLETED}, &reply)
				if err != nil {
					break
				}
			}
		}

	}

}
func WorkerMap(workerId int, serverAddress string, intermediateFilesChan chan string) {
	client, err := rpc.Dial("tcp", serverAddress)
	if err != nil {
		fmt.Println("could not dial server")
		return
	}
	for {
		// Request job
		jobRequestArgs := workerId
		jobRequestReply := Job{}
		err = client.Call("CoordinatorServer.RequestJob", jobRequestArgs, &jobRequestReply)
		if err != nil {
			break
		}
		// Perform Job (or 10 second timeout error)
		fmt.Printf("Processing filename: %v\n", jobRequestReply.Filename)
		filename := jobRequestReply.Filename
		result, err := Map(filename)
		if err != nil {
			break
		}
		// write to intermediate file
		fileNames := WriteIntermediateFile(result, 3, workerId)
		fmt.Println(fileNames) // reduce status map ma halnu paryo
		for _, val := range fileNames {
			intermediateFilesChan <- val
		}
		// Update Coordinator
		var reply int = 5
		err = client.Call("CoordinatorServer.UpdateJobStatus", JobRequest{workerId, filename, JOB_COMPLETED}, &reply)
		if err != nil {
			break
		}
	}
}
