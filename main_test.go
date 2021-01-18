package main

import (
	"io/ioutil"
	"log"
	"testing"
)

func init() {
	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)

	inputDir = "testdata/input"
	outputDir = "testdata/output"
}

func Benchmark(b *testing.B) {
	for i := 0; i < b.N; i++ {
		initiate()
	}
}
func initiate() {
	mainStream := make(chan string)
	closeSignal := make(chan struct{})
	circuitBreaker := make(chan struct{})
	defer func() {
		close(circuitBreaker)
	}()

	go producer(inputDir, *numberOfWorker, mainStream, circuitBreaker)

	go consumer(mainStream, closeSignal)

	<-closeSignal
}
