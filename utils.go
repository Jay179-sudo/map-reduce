package main

import (
	"fmt"
	"os"
	"sync"
)

func (coordinator *CoordinatorServer) Cleanup() {
	wg := sync.WaitGroup{}
	wg.Add(2)

	go CreateReducedFile(&wg)
	go DeleteIntermediateFiles(&wg, coordinator.ReduceStatus)

	wg.Wait()

}
func CreateReducedFile(wg *sync.WaitGroup) {

	reducedFile, err := os.Create(ABS_FILE_PATH + "reduced" + ".txt")
	if err != nil {
		fmt.Println("Could not create reduced file")
		return
	}

	for i := range 3 {
		// replace 3 with const
		fileName := fmt.Sprintf("reduced-%v.txt", i)
		content, err := os.ReadFile(ABS_FILE_PATH + fileName)
		if err != nil {
			fmt.Println("Could not write to reduce file")
			return
		}
		reducedFile.Write(content)
		os.Remove(ABS_FILE_PATH + fileName)
	}
	reducedFile.Close()
	wg.Done()

}

func DeleteIntermediateFiles(wg *sync.WaitGroup, reduceStatus map[string]string) {

	fmt.Println("Cleaning up intermediate files...")
	for key := range reduceStatus {
		os.Remove(ABS_FILE_PATH + key + ".txt")
	}
	wg.Done()

}
