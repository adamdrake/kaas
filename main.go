package main

import (
	"bytes"
	"fmt"
	redis "gopkg.in/redis.v2"
	msgpack "gopkg.in/vmihailenco/msgpack.v2"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"errors"
	"io"
)

type graphiteEncoder struct {
	encoder *msgpack.Encoder
	buffer  *bytes.Buffer
}

// Graphite specific encoder backed by a message pack encoder.
// As message packe encoder has private fields, we need to wrap
// instead of just extending.
func newGraphiteEncoder() *graphiteEncoder {
	buf := &bytes.Buffer{}
	return &graphiteEncoder{buffer: buf, encoder: msgpack.NewEncoder(buf)}
}

// Encodes the equivalent of a tuple containing two items, a int64 and a float64.
func (e *graphiteEncoder) encodeInt64Float64Tuple(unixTimestamp int64, value float64) error {
	if err := e.encoder.EncodeSliceLen(2); err != nil {
		return err
	}
	if err := e.encoder.EncodeInt64(unixTimestamp); err != nil {
		return err
	}
	if err := e.encoder.EncodeFloat64(value); err != nil {
		return err
	}
	return nil
}

type graphiteDecoder msgpack.Decoder

func newGraphiteDecoder(r io.Reader) *graphiteDecoder {
	return (*graphiteDecoder)(msgpack.NewDecoder(r))
}

func (d *graphiteDecoder) decodeInt64Float64Tuple() (int64, float64, error){
	md := (*msgpack.Decoder)(d)
	s, err := md.DecodeSliceLen()
	if err != nil {
		return 0, 0, err;
	}
	if s != 2 {
		return 0, 0, errors.New("not a list of size 2")
	}
	i, err := md.DecodeInt64()
	if err != nil {
		return 0,0, err
	}
	f, err := md.DecodeFloat64()
	if err != nil {
		return 0, 0, err
	}
	return i, f, nil
}


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
	defer wg.Done()
	pipe := client.Pipeline()

	var pipecount uint32 = 0
	encoder := newGraphiteEncoder()
	for item := range inq {
		message := string(item)
		messageCleaned := strings.TrimSuffix(message, "\n")
		splitLine := strings.Split(messageCleaned, " ")
		val, _ := strconv.ParseFloat(splitLine[1], 64)
		ts, _ := strconv.ParseInt(splitLine[2], 10, 64)
		encoder.buffer.Reset()
		check(encoder.encodeInt64Float64Tuple(ts, val))
		mpval := encoder.buffer.Bytes()
		pipe.Append(splitLine[0], string(mpval))
		pipe.SAdd("metricNames", splitLine[0])
		pipecount += 1

		if pipecount > 512 {
			cmds, err := pipe.Exec()
			if (*loopcount)%10000 == 0 {
				fmt.Println(cmds)
			}
			check(err)
			pipecount = 0
		}

		if *loopcount%10000 == 0 {
			fmt.Println("rate:", float64(*loopcount)/time.Since(*loopstart).Seconds())
		}

		*loopcount += 1
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

func metricTrimmerLoop(maxlen int, client *redis.Client, interval int) {
	for {
		metricNames, err := client.SMembers("metricNames").Result()
		check(err)
		for _, name := range metricNames {
			rawvals, err := client.Get(name).Result()
			check(err)
			trimmed, err := trimMetrics(maxlen, strings.NewReader(rawvals))
			check(err)
			client.Set(name, string(trimmed))
		}
		time.Sleep(time.Second * time.Duration(interval))
	}
}

func trimMetrics(maxlen int, r io.Reader) ([]byte, error){
	var err error
	var i int = 0
	d := newGraphiteDecoder(r)
	e := newGraphiteEncoder()
	for err != io.EOF && i < maxlen {
		iVal, fVal, err := d.decodeInt64Float64Tuple()
		if err != nil && err != io.EOF {
			return nil, err
		}
		err = e.encodeInt64Float64Tuple(iVal, fVal)
		i++
	}
	return e.buffer.Bytes(), nil
}


func main() {
	startTime := time.Now()

	WORKER_COUNT := runtime.NumCPU() * 2
	MAX_METRICS := 5000
	TRIM_INTERVAL := 1

	runtime.GOMAXPROCS(runtime.NumCPU())

	logFile, err := os.OpenFile("info.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	defer logFile.Close()
	check(err)
	logger := log.New(logFile, "", log.LstdFlags)
	logger.Println("starting execution at", startTime)

	//client := redis.NewClient(&redis.Options{Network: "unix", Addr: "/tmp/redis.sock"})
	client := redis.NewClient(&redis.Options{Network: "tcp", Addr: "localhost:6379"})
	defer client.Close()

	inq := make(chan []byte, 1000000)
	mets := make(chan Metric, 1000000)
	go startListening(inq, mets)

	loopstart := time.Now()
	var loopcount uint64
	var wg sync.WaitGroup

	for i := 0; i < WORKER_COUNT; i++ {
		wg.Add(1)
		go handleMetric(inq, mets, client, &loopcount, &loopstart, &wg)
	}

	for i := 0; i < WORKER_COUNT; i++ {
		wg.Add(1)
		go metricTrimmerLoop(MAX_METRICS, client, TRIM_INTERVAL)
	}

	wg.Wait()
}
