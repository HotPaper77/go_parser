package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/hex"
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
	outputFilename = "count_output.csv"
)

var (
	maxWorkers      = runtime.NumCPU()
	stockColumnName string
	storeColumnName string
	sourceDir       string
	incoming        chan Task
	done            chan Task
	workersWg       sync.WaitGroup
	wg              sync.WaitGroup
)

type Task struct {
	path   string
	result summary
}

type summary map[string]struct {
	totalProducts int
	inStock       int
	outOfStock    int
}

type parseError struct {
	line int
	err  error
}

func (e *parseError) Error() string {
	errorMessage := fmt.Sprintf("could not parse record line %d. error:%v", e.line, e.err)
	return errorMessage
}

type noStockColumn struct{}

func (e *noStockColumn) Error() string {
	errorMessage := fmt.Sprintf("column %s not present in file", stockColumnName)
	return errorMessage
}

type noStoresColumn struct {
}

func (noStoresColumn) Error() string {

	errorMessage := fmt.Sprintf("column %s not present in file", storeColumnName)

	return errorMessage
}

func (t *Task) Write(w *bufio.Writer) {

	for store, data := range t.result {
		message := fmt.Sprintf("%s,%s,%d,%d,%d\n", t.path, store, data.inStock, data.outOfStock, data.totalProducts)
		_, err := w.WriteString(message)

		if err != nil {
			log.Fatal(err)
		}
	}
}

func worker(incoming chan Task, done chan Task) {
	for t := range incoming {

		err := process(t.path, &t)

		if err != nil {
			fmt.Printf("Could not process file %s. Error: %v\n", t.path, err)
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
	flag.StringVar(&storeColumnName, "store_id", "store_id", "enter the name of the column store_id. default is 'store_id'")

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

	outputFileHeader := "filepath,store_id,inStock,outOfStock,totalProducts\n"

	_, err = writer.WriteString(outputFileHeader)

	if err != nil {
		log.Fatal(err)
	}

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

	storesSummary := make(map[string]struct {
		totalProducts int
		inStock       int
		outOfStock    int
	})

	t.result = storesSummary

	fd, err := os.Open(path)

	if err != nil {
		return err
	}

	br := bufio.NewReader(fd)

	bom, _ := br.Peek(3)

	bomHex, err := hex.DecodeString("EFBBBF")

	if err != nil {
		panic(err)
	}

	if bytes.Equal(bom, bomHex) {
		_, err := br.Discard(3)

		if err != nil {
			return err
		}
	}

	r := csv.NewReader(br)

	records, err := r.ReadAll()

	if err != nil {
		return err
	} else if len(records) == 0 {
		// if there are no records in a given file create a dummy store 'NAN' and assign null values
		t.result["NAN"] = struct {
			totalProducts int
			inStock       int
			outOfStock    int
		}{0, 0, 0}
		return nil
	}

	header := records[0]
	headerMap := mapHeader(header)

	for i, record := range records[1:] {
		var store_id string
		var storeInfo struct {
			totalProducts int
			inStock       int
			outOfStock    int
		}
		if indexColumnStore, ok := headerMap[storeColumnName]; ok {
			store_id = record[indexColumnStore]
		} else {
			newError := noStoresColumn{}
			return &newError
		}

		if summary, ok := t.result[store_id]; !ok {

			storeInfo = struct {
				totalProducts int
				inStock       int
				outOfStock    int
			}{0, 0, 0}

			t.result[store_id] = storeInfo
		} else {
			storeInfo = summary
		}
		if indexColumnStock, ok := headerMap[stockColumnName]; ok {

			stockValue, err := strconv.ParseFloat(record[indexColumnStock], 32)

			if err != nil {
				newError := parseError{line: i, err: err}
				return &newError
			}

			if stockValue > 0 {
				storeInfo.inStock++
			} else if stockValue <= 0 {
				storeInfo.outOfStock++
			}

		} else {
			newError := noStockColumn{}
			return &newError
		}
		storeInfo.totalProducts++
		t.result[store_id] = storeInfo
	}

	return nil
}

func mapHeader(header []string) map[string]int {

	headerMap := make(map[string]int)

	for index, columName := range header {
		headerMap[columName] = index
	}

	return headerMap
}
