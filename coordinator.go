package main

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const JOB_IDLE string = "JOB_IDLE"
const JOB_PROGRESS string = "JOB_PROGRESS"
const JOB_COMPLETED string = "JOB_COMPLETED"

var JOB_NOT_FOUND = errors.New("NO JOB AVAILABLE FOR PROCESSING")

type Job struct {
	Filename string
}

type JobRequest struct {
	WorkerId   int
	Filename   string
	Job_Status string
}
type CoordinatorServer struct {
	Address   string
	JobStatus map[string]string // we will use filenames to identify jobs to map
	// WorkerStatus map[int]string    // identify workers with the help of integer IDs
	mu           sync.Mutex        // Protection from concurrent acccess
	ReduceStatus map[string]string // intermediate filenames to identify reduce jobs
	MapOver      bool
}

func (server *CoordinatorServer) Listen() {
	err := rpc.Register(server)
	if err != nil {
		fmt.Println("Error Detected, exiting process ", err)
		return
	}
	listener, err := net.Listen("tcp", server.Address)
	if err != nil {
		fmt.Println("Error Detected, exiting process. Could not start TCP Listener")
		return
	}
	go func() {
		rpc.Accept(listener)
	}()

}

// RequestJob: func(id int, reply) (JobReference, error)
// UpdateJobStatus: func(id (wokerId) int, Job_Id int, Job_Status int) (nil) still need to thinkk about output. Intermediary files ko barema sochnu paryo

func (coordinator *CoordinatorServer) RequestJob(workerId int, reply *Job) error {

	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	// find job
	jobFound := Job{""}
	for key, value := range coordinator.JobStatus {
		if value == JOB_IDLE {
			jobFound.Filename = key
			coordinator.JobStatus[key] = JOB_PROGRESS
			break
		}
	}
	if jobFound.Filename == "" {
		return JOB_NOT_FOUND
	}
	reply.Filename = jobFound.Filename
	return nil
}

func (coordinator *CoordinatorServer) UpdateJobStatus(request JobRequest, reply *int) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.JobStatus[request.Filename] = JOB_COMPLETED
	*reply = 5
	return nil
}

func (coordinator *CoordinatorServer) UpdateReduceStatus(request JobRequest, reply *int) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.ReduceStatus[request.Filename] = JOB_COMPLETED
	*reply = 5
	return nil
}

func (coordinator *CoordinatorServer) CheckReducePhaseOver() chan bool {
	ch := make(chan bool)
	go func() {
		for {
			coordinator.mu.Lock()
			result := true
			for _, value := range coordinator.ReduceStatus {
				if value != JOB_COMPLETED {
					result = false
				}
			}
			if result {
				coordinator.mu.Unlock()
				ch <- true
				// break garnu paryo
			} else {
				coordinator.mu.Unlock()
				time.Sleep(1 * time.Second)
			}
		}
	}()

	return ch
}
func (coordinator *CoordinatorServer) CheckMapPhaseOver() chan bool {
	ch := make(chan bool)
	go func() {
		for {
			coordinator.mu.Lock()
			result := true
			for _, value := range coordinator.JobStatus {
				if value != JOB_COMPLETED {
					result = false
				}
			}
			if result {
				coordinator.mu.Unlock()
				ch <- true
				// break garnu paryo
			} else {
				coordinator.mu.Unlock()
				time.Sleep(1 * time.Second)
			}
		}
	}()
	return ch
}

// -------------------- REDUCE --------------------------------
type ReduceJob struct {
	Filenames []string
}

func (coordinator *CoordinatorServer) ReduceRequest(workerId int, reply *ReduceJob) error {
	workers := 3
	filenames := make([]string, 0)
	coordinator.mu.Lock()
	// no you will not be iterating over all the values. Iterate over <workers><workerID>
	for index := range workers {
		filenameStr := fmt.Sprintf("file-%v-%v", index+1, workerId)
		if coordinator.ReduceStatus[filenameStr] == JOB_IDLE {
			filenames = append(filenames, filenameStr)
		}
	}
	reply.Filenames = filenames
	coordinator.mu.Unlock()
	return nil
}
