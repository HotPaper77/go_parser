package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const (
	outputFilename = "count_output.txt"
)

var (
	maxWorkers      = runtime.NumCPU()
	stockColumnName string
	sourceDir       string
	incoming        chan Task
	done            chan Task
	workersWg       sync.WaitGroup
	wg              sync.WaitGroup
)

type Task struct {
	path          string
	totalProducts int
	inStock       int
}

type parseError struct {
	line int
	err  error
}

func (e *parseError) Error() string {
	errorMessage := fmt.Sprintf("could not parse record line %d. error:%v\n", e.line, e.err)
	return errorMessage
}

type noStockColumn struct{}

func (e *noStockColumn) Error() string {
	errorMessage := fmt.Sprintln("column stock not present in file")
	return errorMessage
}

func (t *Task) Write(w *bufio.Writer) {
	message := fmt.Sprintf("%s,%d,%d\n", t.path, t.totalProducts, t.inStock)

	_, err := w.WriteString(message)

	if err != nil {
		log.Fatal(err)
	}
}

func worker(incoming chan Task, done chan Task) {
	for t := range incoming {

		err := process(t.path, &t)

		if err != nil {
			fmt.Printf("Could not process file %s. Error: %v", t.path, err)
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

func init() {
	flag.StringVar(&stockColumnName, "stock", "stock", "enter the name of the column stock. default is 'stock'")
}

func main() {

	start := time.Now()

	sourceDir = os.Args[1]

	log.Println(sourceDir)

	absPath, err := filepath.Abs(sourceDir)

	log.Printf("Input Directory Path: %s.\n Workers:%d\n", sourceDir, maxWorkers)

	if err != nil {
		panic(err)
	}

	incoming = make(chan Task, 50)

	done = make(chan Task, 200)

	file, err := os.Create(outputFilename)

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Writing to output file: %s\n", outputFilename)

	writer := bufio.NewWriter(file)

	defer writer.Flush()

	wg.Add(1)
	go func(w *bufio.Writer) {
		defer wg.Done()
		for t := range done {
			t.Write(w)
			//fmt.Printf("%s,%d\n", t.path, t.result)
		}
	}(writer)

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

	executionTime := time.Since(start)

	log.Printf("Parsing Completed!\n Execution Time:%9.2f sec\n", executionTime.Seconds())

}

func process(path string, t *Task) error {

	inStockCount := 0

	fd, err := os.Open(path)

	if err != nil {
		return err
	}

	r := csv.NewReader(fd)

	records, err := r.ReadAll()

	if err != nil {
		return err
	} else if len(records) == 0 {
		t.totalProducts = 0
		t.inStock = 0
		return nil
	}

	header := records[0]
	headerMap := mapHeader(header)

	for i, record := range records[1:] {

		if indexColumnStock, ok := headerMap[stockColumnName]; ok {

			stockValue, err := strconv.ParseFloat(record[indexColumnStock], 32)

			if err != nil {
				newError := parseError{line: i, err: err}
				return &newError
			}

			if stockValue > 0 {
				inStockCount++
			}

		} else {
			newError := noStockColumn{}
			return &newError
		}

	}
	t.totalProducts = len(records) - 1
	t.inStock = inStockCount

	return nil
}

func mapHeader(header []string) map[string]int {

	headerMap := make(map[string]int)

	for index, columName := range header {
		headerMap[columName] = index
	}

	return headerMap
}
