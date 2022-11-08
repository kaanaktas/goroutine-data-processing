package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

func elapsed(what string) func() {
	start := time.Now()
	return func() {
		log.Printf("%s took %v\n", what, time.Since(start))
	}
}

var cpuProfile = flag.Bool("cpu-profile", false, "write cpu profile to `file`")
var memoryProfile = flag.Bool("mem-profile", false, "write memory profile to `file`")
var numberOfWorker = flag.Int("worker-number", 1, "set number of worker")

var inputDir = "data/input"
var outputDir = "data/output"

func main() {
	defer func() {
		log.Println("Process is complete successfully! Number of Active GoRoutine:", runtime.NumGoroutine())
	}()
	defer elapsed("Execution")()

	flag.Parse()

	if *cpuProfile {
		f := startCPUTrace()
		defer f()
	}

	if *memoryProfile {
		f := startMemoryProfile()
		defer f()
	}

	log.Printf("Process is starting with %d worker...\n", *numberOfWorker)

	//mainstream channel is the main channel that carries events from producer to consumer. It receives events during the Fan In operation
	mainStream := make(chan string)
	//closeSignal is managed by consumer and It blocks until consumer sends close signal over it
	closeSignal := make(chan struct{})

	//breaker connects all stages in the pipeline and ensures all working goroutines stopped and open channels closed, after close signal
	//It is expected that close signal will always be sent from main function defer method
	//Main purpose here is to prevent memory leak, because goroutines are not garbage collected; they must exit on their own.
	breaker := make(chan struct{})
	defer func() {
		close(breaker)
		log.Println("breaker is closed. Number of Active GoRoutine:", runtime.NumGoroutine())
	}()

	go producer(inputDir, *numberOfWorker, mainStream, breaker)

	go consumer(mainStream, closeSignal)

	<-closeSignal
}

func producer(sourceDir string, numberOfWorker int, mainStream chan<- string, breaker <-chan struct{}) {
	defer func() {
		close(mainStream)
		log.Println("mainStream is closed. Number of Active GoRoutine:", runtime.NumGoroutine())
	}()

	var wg sync.WaitGroup

	var walk = func(currentPath string, fileInfo os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error %v source directory: %q\n", err, currentPath)
		}

		if !fileInfo.IsDir() {
			if filepath.Ext(currentPath) == ".log" {
				wg.Add(1)
				go func() {
					defer func() {
						defer wg.Done()
						log.Printf("FileParser is closed for %q. Number of Active GoRoutine: %d\n", fileInfo.Name(), runtime.NumGoroutine())
					}()

					filePath := filepath.Dir(currentPath) + "\\" + fileInfo.Name()
					log.Println("Reading file:", filePath)
					consent, err := ioutil.ReadFile(filePath)
					if err != nil {
						log.Printf("error reading the file %q: %v\n", filePath, err)
					}

					processEvents(strings.Split(string(consent), "\n"), fileInfo.Name(), numberOfWorker, mainStream, breaker)
				}()
			}
		}

		return nil
	}

	err := filepath.Walk(sourceDir, walk)
	wg.Wait()

	if err != nil {
		log.Printf("error walking the path %q: %v\n", sourceDir, err)
		os.Exit(1)
	}
}

func processEvents(content []string, filename string, numberOfWorker int, mainStream chan<- string, breaker <-chan struct{}) {
	in := make(chan string)
	go func() {
		defer func() {
			close(in)
			log.Printf("processEvents is closed for %q. Number of Active GoRoutine: %d\n", filename, runtime.NumGoroutine())
		}()

		for _, c := range content {
			if strings.Trim(c, " ") != "" {
				in <- c
			}
		}
	}()

	//Distribute work to multiple workers to implement FAN OUT pattern
	//Added worker id and the file name to see tagged process
	var workerList = make([]<-chan string, numberOfWorker)
	for i := 0; i < numberOfWorker; i++ {
		wName := fmt.Sprintf("w%d_%s", i+1, filename)
		workerList[i] = worker(wName, in, breaker)
	}

	mergeWorkers(workerList, mainStream, breaker)
}

func worker(name string, in <-chan string, breaker <-chan struct{}) <-chan string {
	out := make(chan string)
	go func() {
		defer func() {
			close(out)
			log.Printf("worker is closed for %q. Number of Active GoRoutine: %d\n", name, runtime.NumGoroutine())
		}()

		for s := range in {
			res, err := convertToJson(name + "|" + s)
			if err == nil {
				select {
				case out <- res:
				case <-breaker:
					return
				}
			}
		}
	}()

	return out
}

func mergeWorkers(cs []<-chan string, mainStream chan<- string, breaker <-chan struct{}) {
	var wg sync.WaitGroup
	wg.Add(len(cs))

	for _, c := range cs {
		go func(ct <-chan string) {
			defer func() {
				wg.Done()
				log.Println("mergeWorkers is closed. Number of Active GoRoutine:", runtime.NumGoroutine())
			}()
			for n := range ct {
				select {
				case mainStream <- n:
				case <-breaker:
					return
				}
			}
		}(c)
	}

	wg.Wait()
}

func consumer(mainStream <-chan string, consumerCloseSignal chan<- struct{}) {
	dataGroup := make(map[string][]string)

	defer func() {
		completeFileOperation(dataGroup)
		close(consumerCloseSignal)
		log.Println("consumer is closed. Number of Active GoRoutine:", runtime.NumGoroutine())
	}()

	var counter int

	for c := range mainStream {
		counter++
		var j logJson
		if err := json.Unmarshal([]byte(c), &j); err != nil {
			log.Printf("Couldn't retrieve Json message err: %v", err)
		}

		groupId := j.Group
		dataGroup[groupId] = append(dataGroup[groupId], c)
	}

	log.Println("TOTAL COUNT:", counter)
}

type logJson struct {
	Obj       string `json:"obj"`
	Id        int64  `json:"id"`
	Section   string `json:"section"`
	Category  string `json:"category"`
	User      string `json:"user"`
	Seed      string `json:"seed"`
	Group     string `json:"group"`
	TimeStamp string `json:"time_stamp"`
}

func convertToJson(record string) (string, error) {
	lines := strings.Split(record, "|")

	id, err := strconv.ParseInt(lines[1], 10, 64)
	if err != nil {
		return "", err
	}

	newLog := &logJson{
		Obj:       lines[0],
		Id:        id,
		Section:   lines[2],
		Category:  lines[3],
		User:      lines[4],
		Seed:      lines[5],
		Group:     lines[6],
		TimeStamp: lines[7],
	}

	jLog, err := json.Marshal(newLog)
	if err != nil {
		return "", err
	}

	return string(jLog), nil
}

func completeFileOperation(dataGroup map[string][]string) {
	closeWriteFile := make(chan struct{})
	defer close(closeWriteFile)

	for k, v := range dataGroup {
		var filename = "output.log"

		if k == "" {
			filename = "ungrouped_" + filename
		} else {
			filename = "group_" + k + "_" + filename
		}

		go writeToFile(outputDir+"/"+filename, v, closeWriteFile)
		<-closeWriteFile
	}
}

func writeToFile(filename string, data []string, closeSignal chan<- struct{}) {
	defer func() {
		closeSignal <- struct{}{}
	}()

	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Error occured while creating the file %v", err)
		return
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range data {
		_, err := fmt.Fprintln(w, line)
		if err != nil {
			log.Printf("error occured while writing single line %q to the file: %v\n", line, err)
		}
	}

	if w.Flush() != nil {
		log.Printf("Writing to file was aborted due an error %v", err)
	} else {
		log.Printf("Data was written to %q successfully.\n", filename)
	}
}
