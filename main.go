package main

import (
	"encoding/csv"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

var (
	maxWorkers = runtime.NumCPU()
	sourceDir  string
	incoming   chan Task
	done       chan Task
	workersWg  sync.WaitGroup
	wg         sync.WaitGroup
)

type Task struct {
	path   string
	result int
}

func worker(incoming chan Task, done chan Task) {
	for t := range incoming {

		err := process(t.path, &t)

		if err != nil {
			fmt.Printf("Could not process %s. Error: %v", t.path, err)
		}

		done <- t

	}
}

///mnt/c/Users/Elvis Gbaguidi/GolandProjects/gftp/Files_Downloaded_from_SFTP/dia-es/2024-02-27T10h42m27s

func Visit(path string, d fs.DirEntry, err error) error {

	if err != nil {
		return err
	}

	if !d.IsDir() {
		newTask := Task{path: path}
		incoming <- newTask
	}

	return nil
}

func main() {

	sourceDir = os.Args[1]

	log.Printf("Input Directory Path: %s\n", sourceDir)

	absPath, err := filepath.Abs(sourceDir)

	if err != nil {
		panic(err)
	}

	incoming = make(chan Task, 200)

	done = make(chan Task, 200)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range done {
			fmt.Printf("%s,%d\n", t.path, t.result)
		}
	}()

	for w := 1; w <= maxWorkers; w++ {
		workersWg.Add(1)
		go func(i chan Task, d chan Task) {
			defer workersWg.Done()
			worker(i, d)
		}(incoming, done)
	}

	err = filepath.WalkDir(absPath, Visit)

	if err != nil {
		fmt.Printf("An Error Occured %v\n", err)
	}

	close(incoming)

	workersWg.Wait()

	close(done)

	wg.Wait()

	log.Println("Parsing Completed!")

}

func process(path string, t *Task) error {

	fd, err := os.Open(path)

	if err != nil {
		return err
	}

	r := csv.NewReader(fd)

	records, err := r.ReadAll()

	if err != nil {
		return err
	}

	t.result = len(records)

	return nil
}
