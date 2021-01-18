package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"time"
)

func startCPUTrace() func() {
	fileName := fmt.Sprintf("cpu-%d.pprof", time.Now().Unix())
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("could not create %s: %v", fileName, err)
	}

	log.Printf("creating CPU profile file %s", fileName)
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatalf("could not start CPU profile: %v", err)
	}

	return func() {
		f.Close()
		pprof.StopCPUProfile()
	}
}

func startMemoryProfile() func() {
	fileName := fmt.Sprintf("mem-%d.trace", time.Now().Unix())
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("could not create %s: %v", fileName, err)
	}
	log.Printf("creating memory profile file %s", fileName)

	runtime.GC() // get up-to-date statistics
	if err := trace.Start(f); err != nil {
		log.Fatalf("could not write memory profile: %v", err)
	}

	return func() {
		trace.Stop()
		f.Close()
	}
}
