package main

import (
	"fmt"
	"net/rpc"
	"os"
	"sync"
	"time"
)

func (coordinator *CoordinatorServer) Cleanup() {
	wg := sync.WaitGroup{}
	wg.Add(2)

	go CreateReducedFile(&wg)
	go DeleteIntermediateFiles(&wg, coordinator.ReduceStatus)

	wg.Wait()
	fmt.Println("Cleanup completed...")

}
func CreateReducedFile(wg *sync.WaitGroup) {

	reducedFile, err := os.Create(ABS_FILE_PATH + "reduced" + ".txt")
	if err != nil {
		fmt.Println("Could not create reduced file")
		return
	}

	for i := range 3 {
		// replace 3 with const
		fileName := fmt.Sprintf(ABS_FILE_PATH+"reduced-%v.txt", i)
		content, err := os.ReadFile(fileName)
		if err != nil {
			fmt.Println("Could not write to reduce file ", err)
			return
		}
		reducedFile.Write(content)
		os.Remove(fileName)
	}
	reducedFile.Close()
	wg.Done()

}

func DeleteIntermediateFiles(wg *sync.WaitGroup, reduceStatus map[string]string) {

	fmt.Println("Cleaning up intermediate files...")
	for key := range reduceStatus {
		fmt.Println(ABS_FILE_PATH + key + ".txt")
		os.Remove(ABS_FILE_PATH + key + ".txt")
	}
	wg.Done()

}

func runHealthCheck(client *rpc.Client) {
	reply := 5

	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-ticker.C:
			// fmt.Println("Performing healthchecks")
			client.Call("CoordinationServer.Healthcheck", WORKER_ID, &reply)
		}
	}

}
