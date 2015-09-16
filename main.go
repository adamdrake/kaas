package main

import (
	"fmt"
	redis "gopkg.in/redis.v2"
	"log"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type Metric struct {
	name        string
	measurement Measurement
}

type Measurement struct {
	value     float64
	timestamp int64
}

type Measurements []Measurement

func handleMetric(inq chan []byte, outq chan Metric, client *redis.Client, loopcount *uint64, loopstart *time.Time, wg *sync.WaitGroup) {
	var MAX_METRICS int64 = 500000
	defer wg.Done()
	pipe := client.Pipeline()

	var pipecount uint32 = 0
	for item := range inq {
		message := string(item)
		messageCleaned := strings.TrimSuffix(message, "\n")
		splitLine := strings.Split(messageCleaned, " ")
		metricName := splitLine[0]
		pipe.RPush(metricName, splitLine[1]+","+splitLine[2])
		pipe.LTrim(metricName, 0, MAX_METRICS)

		pipe.SAdd("metricNames", metricName)
		pipecount++

		if pipecount > 512 {
			_, err := pipe.Exec()
			check(err)
			pipecount = 0
		}

		if *loopcount%10000 == 0 {
			fmt.Println("rate:", float64(*loopcount)/time.Since(*loopstart).Seconds())
		}

		*loopcount++
	}
}

func startListening(inq chan []byte, outq chan Metric) {
	addr, _ := net.ResolveUDPAddr("udp", ":2001")
	sock, err := net.ListenUDP("udp", addr)
	check(err)
	for {
		buf := make([]byte, 512)
		size, _, err := sock.ReadFromUDP(buf)
		check(err)
		inq <- buf[0:size]
	}
}

func main() {
	startTime := time.Now()
	WORKER_COUNT := runtime.NumCPU() * 2

	logFile, err := os.OpenFile("info.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	defer logFile.Close()
	check(err)
	logger := log.New(logFile, "", log.LstdFlags)
	logger.Println("starting execution at", startTime)

	client := redis.NewClient(&redis.Options{Network: "tcp", Addr: "localhost:6379"})
	defer client.Close()

	inq := make(chan []byte)
	mets := make(chan Metric)
	go startListening(inq, mets)

	loopstart := time.Now()
	var loopcount uint64
	var wg sync.WaitGroup

	for i := 0; i < WORKER_COUNT; i++ {
		wg.Add(1)
		go handleMetric(inq, mets, client, &loopcount, &loopstart, &wg)
	}

	wg.Wait()
}
