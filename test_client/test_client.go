package main

import (
	"math/rand"
	"net"
	"runtime"
	"strconv"
	"sync"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func spawnClient(metricNames []string, port string, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	ServerAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:"+port)
	check(err)

	LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	check(err)

	Conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
	check(err)

	defer Conn.Close()
	for {
		timestamp := strconv.FormatInt(time.Now().UnixNano(), 10)
		val := strconv.FormatFloat(rand.Float64(), 'f', 16, 64)
		name := metricNames[rand.Intn(len(metricNames))]

		msg := []byte(name + " " + val + " " + timestamp + " \n")
		Conn.Write(msg)
		time.Sleep(time.Millisecond * 1)
	}
}

func randMetricName(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	THOUSAND_METS_PER_SEC := 30
	METRIC_NAME_LENGTH := 32
	NUM_METRICS := 500000
	PORT := "2001"

	var metricsList []string
	for i := 0; i < NUM_METRICS; i++ {
		metricsList = append(metricsList, randMetricName(METRIC_NAME_LENGTH))
	}

	var wg sync.WaitGroup
	// spawnClient(metricsList, PORT, &wg)

	for i := 0; i < THOUSAND_METS_PER_SEC; i++ {
		go spawnClient(metricsList, PORT, &wg)
	}

	wg.Wait()
}
