package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

var ERRORREADING = errors.New("COULD NOT READ VALUE")

type MapResult struct {
	Word  string
	Count int
}

func Reduce(filenames []string, reducedMap map[string]int, workerId int) bool {
	fmt.Println("Reduction on: ", filenames)
	for _, element := range filenames {
		file, err := os.Open(ABS_FILE_PATH + "\\" + element + ".txt")
		if err != nil {
			fmt.Println("Error while reading file ", err)
			return false
		}
		defer file.Close()
		scanner := bufio.NewReader(file)
		for {
			read, err := scanner.ReadString('\n')
			if err != nil && err != io.EOF {
				fmt.Println("Error while reading file ", err)
				return false
			} else if err == io.EOF {
				break
			}
			line := strings.Split(read, ",")
			reducedMap[line[0]]++
		}

	}
	WriteReduceOutputFile(reducedMap, workerId)
	return true
	// write reduce function to an output file
}

func WriteReduceOutputFile(reducedMap map[string]int, workerId int) {
	fileRefStr := fmt.Sprintf("reduced-%v.txt", workerId)
	// fileRef, _ := os.OpenFile(fileRefStr, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	fileRef, _ := os.Create(fileRefStr)
	for key, value := range reducedMap {
		textAppend := fmt.Sprintf("%v,%d\n", key, value)
		fileRef.Write([]byte(textAppend))
	}
}
func Map(filename string) ([]MapResult, error) {
	// this will take every word in the document and will produce a
	// <word, 1> pair for every word
	result := make([]MapResult, 0)
	filePath := fmt.Sprintf("%v", filename)
	data, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Println(err)
		return nil, ERRORREADING
	}
	words := strings.Fields(string(data))
	for _, element := range words {
		result = append(result, MapResult{element, 1})
	}
	return result, nil
}

func Hashing(word string, workers int) int {
	sum := 0
	for _, element := range word {
		sum += int(element)
	}
	return sum % workers
}
func WriteIntermediateFile(result []MapResult, workers int, workerId int) {
	fileRefs := make([]*os.File, workers)
	for index := range workers {
		file_name := fmt.Sprintf("%v\\file-%d-%d.txt", ABS_FILE_PATH, workerId, index)
		filePtr, err := os.OpenFile(file_name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println("Error opening file ", err)
			return
		}
		fileRefs[index] = filePtr
	}

	for _, element := range result {
		fileHandler := Hashing(element.Word, workers)
		textAppend := fmt.Sprintf("%v,%d\n", element.Word, element.Count)
		fmt.Printf("WorkerId: %v File :%v Word :%v\n", workerId, fileHandler, textAppend)
		fileRefs[fileHandler].Write([]byte(textAppend))
	}

	for _, element := range fileRefs {
		element.Close()
	}
}
