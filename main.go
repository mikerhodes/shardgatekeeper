package main

import (
	"flag"
	"log"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/theodesp/blockingQueues"
)

const ClientTypeWriter = "writer"
const ClientTypeReader = "reader"

func main() {

	gatekeeper := flag.String("g", "serial", "gatekeeper: null, serial")
	jobsPerSecond := flag.Float64("j", 1000, "jobs per second target")
	readWorkers := flag.Int("r", 10, "read worker goroutines")
	writeWorkers := flag.Int("w", 10, "write worker goroutines")
	seconds := flag.Int("s", 1, "duration of test in seconds")
	writePercentage := flag.Int("p", 20, "percentage writes")
	flag.Parse()

	// TODO have seconds be a parsed time string and figure out
	//      how to calculate the totalJobs from that?

	runTime := time.Duration(*seconds) * time.Second

	tickerSleep := time.Duration(float64(1*time.Second) / *jobsPerSecond)

	log.Printf("seconds:         %d", *seconds)
	log.Printf("jobsPerSecond:   %f", *jobsPerSecond)
	log.Printf("(total jobs      %d)", *seconds*int(*jobsPerSecond))
	log.Printf("tickerSleep:     %s", tickerSleep)
	log.Printf("read workers:    %d", *readWorkers)
	log.Printf("write workers:   %d", *writeWorkers)
	log.Printf("writePercentage: %d", *writePercentage)
	log.Printf("gatekeeper:      %s", *gatekeeper)

	// Our backend mock shard.
	shard := &Shard{
		store: make(map[string]int64),
	}

	// The Gatekeeper defines how we are allowing reads and writes
	// to be send to the shard. Eg, the SerialGatekeeper puts all
	// the client read/writes into a single queue and executes them
	// in the order they were added to that queue.
	var gk Gatekeeper
	switch *gatekeeper {
	case "null":
		// Null case, read/write direct to shard. While each read or
		// write will get a time.Sleep and so take a while, there can
		// be thousands in flight at a time if you have enough workers.
		// And the actual read/write on the shard's backing map is
		// super-fast, even with the mutex protecting it. It can handle
		// over 200,000 operations a second with enough workers.
		// This mostly just tests that, with enough workers, we can
		// achieve higher throughput than we need to do this simulation,
		// the the backend shouldn't be a bottleneck.
		gk = &NullGatekeeper{shard: shard}
	case "serial":
		// Serial Gateway -- for now make the queue really big.
		queue, _ := blockingQueues.NewArrayBlockingQueue(10_000_000)
		gk = &SerialGatekeeper{
			queue: queue,
			s:     shard,
		}
		go gk.Start()
	case "parallel":
		// Serial Gateway -- for now make the queue really big.
		gk = NewParallelGatekeeper(shard)
		go gk.Start()
	default:
		panic("Bad gatekeeper")
	}

	// Channel for letting workers new they should work
	readWorkC := make(chan bool)
	writeWorkC := make(chan bool)

	// We expect one result from each of the read and write workers
	resC := make(chan clientResult, *readWorkers+*writeWorkers)

	// Clients exit when workC is closed.
	clientId := 0
	for i := 0; i < *readWorkers; i++ {
		clientId += 1
		go readClient(gk, readWorkC, resC, clientId)
	}
	for i := 0; i < *writeWorkers; i++ {
		clientId += 1
		go writeClient(gk, writeWorkC, resC, clientId)
	}

	ticker := time.NewTicker(tickerSleep)
	defer ticker.Stop()

	stopWorkT := time.NewTimer(runTime)

	droppedReads := 0
	droppedWrites := 0
	submittedWrites := 0
	submittedReads := 0

	start := time.Now()

	dw := NewDoWrite(*writePercentage)

	stop := false
	for !stop {
		select {
		case <-ticker.C:
			var c chan bool
			write := dw.Next()
			if write {
				c = writeWorkC
				submittedWrites += 1
			} else {
				c = readWorkC
				submittedReads += 1
			}
			select {
			case c <- true:
				// message sent
			default:
				// message dropped
				if write {
					droppedWrites += 1
				} else {
					droppedReads += 1
				}
			}
		case <-stopWorkT.C:
			stop = true
		}
	}

	close(readWorkC)
	close(writeWorkC)

	log.Println("======================")

	readerSummary := clientResult{clientType: ClientTypeReader}
	writerSummary := clientResult{clientType: ClientTypeWriter}

	// Create reader/writer summaries
	for i := 0; i < *readWorkers+*writeWorkers; i++ {
		result := <-resC
		// log.Printf("%+v", result)

		var summary *clientResult
		switch result.clientType {
		case ClientTypeReader:
			summary = &readerSummary
		case ClientTypeWriter:
			summary = &writerSummary
		}
		summary.statusConflict += result.statusConflict
		summary.statusCreated += result.statusCreated
		summary.statusNotFound += result.statusNotFound
		summary.statusOK += result.statusOK
		summary.statusUnknown += result.statusUnknown
		summary.total = summary.total + result.total
	}
	close(resC)

	log.Printf("Reader summary: %+v", readerSummary)
	log.Printf("Writer summary: %+v", writerSummary)
	log.Println("======================")
	log.Printf("Time taken: %s", time.Now().Sub(start))
	log.Printf("Total submitted reads:     %d", submittedReads)
	log.Printf("Total dropped reads:       %d", droppedReads)
	log.Printf("Accepted read throughput:  %.0f req/s",
		float64(submittedReads-droppedReads)/time.Now().Sub(start).Seconds())
	log.Printf("Total submitted writes:     %d", submittedWrites)
	log.Printf("Total dropped writes:       %d", droppedWrites)
	log.Printf("Accepted write throughput:  %.0f req/s",
		float64(submittedWrites-droppedWrites)/time.Now().Sub(start).Seconds())
	log.Println("======================")
	log.Println("Client summaries:")
	log.Printf("Successful reads:     %d", readerSummary.statusOK)
	log.Printf("Failed reads:         %d", readerSummary.statusNotFound)
	log.Printf("Successful write %%:   %.2f",
		float64(writerSummary.statusCreated)/float64(writerSummary.total))
	log.Printf("Conflict write %%:     %.2f",
		float64(writerSummary.statusConflict)/float64(writerSummary.total))
}

// DoWrite allows clients to know when to do a write
// for a given percentage of writes.
type DoWrite struct {
	writeTable []bool
	doWriteIdx int
}

// NewDoWrite creates a DoWrite that will return writes
// writePercentable amount of the time. The passed write
// percentage is clamped to 0 <= writePercentage <= 100.
func NewDoWrite(writePercentage int) *DoWrite {
	if writePercentage < 0 {
		writePercentage = 0
	}
	if writePercentage > 100 {
		writePercentage = 100
	}
	// r := rand.New(rand.NewPCG(123, 456))

	// Create a shuffled lookup table for when we
	// should be doing writes. doWriteIdx indexes
	// into this for each loop. Should be fast enough
	// to keep up with most jobs per second.
	doWrite := make([]bool, 100)
	for i := 0; i < writePercentage; i++ {
		doWrite[i] = true
	}
	rand.Shuffle(len(doWrite), func(i, j int) {
		doWrite[i], doWrite[j] = doWrite[j], doWrite[i]
	})
	return &DoWrite{
		writeTable: doWrite,
	}
}

// Next returns true when the caller should do a write
// in order to meet the percentage writes goal.
func (d *DoWrite) Next() bool {
	r := d.writeTable[d.doWriteIdx]
	d.doWriteIdx++
	if d.doWriteIdx >= 100 {
		d.doWriteIdx = 0
	}
	return r
}

// Gatekeeper mediates requests to a Shard.
type Gatekeeper interface {
	Get(req *GetRequest)
	Set(req *SetRequest)
	Start()
}

// clientResult stores statistics for a client run.
type clientResult struct {
	clientType                                                                                                     string
	clientId, total, statusOK, statusNotFound, statusCreated, statusConflict, statusTooManyRequests, statusUnknown int64
}

// updateR updates r using values from resp
func updateR(r *clientResult, resp Response) {
	r.total += 1
	switch resp.code {
	case http.StatusOK:
		r.statusOK += 1
	case http.StatusNotFound:
		r.statusNotFound += 1
	case http.StatusCreated:
		r.statusCreated += 1
	case http.StatusConflict:
		r.statusConflict += 1
	case http.StatusTooManyRequests:
		r.statusTooManyRequests += 1
	default:
		r.statusUnknown += 1
	}
}

// readClient mimics a reading client from the shard backend.
//
// Every time its able to read workC it will do a Get on shard.
// When workC is closed, readClient will return statistics about
// its run as a single value on resC.
func readClient(shard Gatekeeper, workC chan bool, resC chan clientResult, clientId int) {
	// log.Printf("[%d] client started", clientId)
	// defer log.Printf("[%d] client stopped", clientId)

	r := clientResult{clientType: ClientTypeReader, clientId: int64(clientId)}

	// Now loop on the channel that tells us to do work
	for workC != nil {
		select {
		case _, ok := <-workC:
			if !ok {
				workC = nil
				break
			}
			// simulate work
			req := &GetRequest{
				id:    "foo",
				respC: make(chan Response, 1),
			}
			shard.Get(req)
			resp := <-req.respC
			// log.Println(resp)

			updateR(&r, resp)
		}
	}
	resC <- r
}

// writeClient mimics a read-modify-update client patterns on the shard backend.
//
// Every time its able to read workC it will do a Get-Set (mimicing
// read-modify-update) on shard.
// When workC is closed, writeClient will return statistics about
// its run as a single value on resC.
func writeClient(shard Gatekeeper, workC chan bool, resC chan clientResult, clientId int) {
	// log.Printf("[%d] client started", clientId)
	// defer log.Printf("[%d] client stopped", clientId)

	r := clientResult{clientType: ClientTypeWriter, clientId: int64(clientId)}

	// Now loop on the channel that tells us to do work
	for workC != nil {
		select {
		case _, ok := <-workC:
			if !ok {
				workC = nil
				break
			}
			// simulate work - read/modify/update

			// Read to get existing doc's rev if
			// it exists.
			get := &GetRequest{
				id:    "foo",
				respC: make(chan Response, 1),
			}
			shard.Get(get)
			resp := <-get.respC

			// If the doc exists, get its rev for
			// the update.
			revId := int64(0)
			if resp.code == http.StatusOK {
				revId = resp.rev
			}

			// Mock doing some work and/or network latency
			// on the response.
			time.Sleep(1 * time.Millisecond)

			// Attempt to create/update the document
			set := &SetRequest{
				id:    "foo",
				rev:   revId,
				respC: make(chan Response, 1),
			}
			shard.Set(set)
			resp = <-set.respC
			// log.Println(resp)

			// For now only record the status of writes
			updateR(&r, resp)
		}
	}
	resC <- r
}

// NullGatekeeper is a simple transparent pass through
// gate keeper.
type NullGatekeeper struct {
	shard *Shard
}

func (g *NullGatekeeper) Get(r *GetRequest) {
	g.shard.Get(r)
}
func (g *NullGatekeeper) Set(r *SetRequest) {
	g.shard.Set(r)
}
func (g *NullGatekeeper) Start() {}
