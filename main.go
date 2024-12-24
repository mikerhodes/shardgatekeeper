package main

import (
	"log"
	"net/http"
	"time"

	"github.com/theodesp/blockingQueues"
)

func main() {

	var jobsPerSecond float64 = 5000
	seconds := 1
	runTime := time.Duration(seconds) * time.Second

	tickerSleep := time.Duration(float64(1*time.Second) / jobsPerSecond)

	// This needs to be (workerSleep / tickerSleep) + 1
	// to avoid drops.
	readWorkers := 10
	writeWorkers := 5

	log.Printf("seconds:       %d", seconds)
	log.Printf("jobsPerSecond: %f", jobsPerSecond)
	log.Printf("(total jobs    %d)", seconds*int(jobsPerSecond))
	log.Printf("tickerSleep:   %s", tickerSleep)
	log.Printf("read workers:  %d", readWorkers)
	log.Printf("write workers: %d", writeWorkers)

	// Our backend mock shard.
	shard := &Shard{
		store: make(map[string]int64),
	}

	// Null case, read/write direct to shard as much
	// paralell as you can.
	// gk := shard

	// Serial Gateway -- for now make the queue really big.
	queue, _ := blockingQueues.NewArrayBlockingQueue(10_000_000)
	gk := &SerialGatekeeper{
		queue: queue,
		s:     shard,
	}
	go gk.Run()

	workC := make(chan bool)

	// We expect one result from each of the read and write workers
	resC := make(chan clientResult, readWorkers+writeWorkers)

	// Clients exit when workC is closed.
	clientId := 0
	for i := 0; i < readWorkers; i++ {
		clientId += 1
		go readClient(gk, workC, resC, clientId)
	}
	for i := 0; i < writeWorkers; i++ {
		clientId += 1
		go writeClient(gk, workC, resC, clientId)
	}

	ticker := time.NewTicker(tickerSleep)
	defer ticker.Stop()

	stopWorkT := time.NewTimer(runTime)

	dropped := 0
	submitted := 0

	start := time.Now()

	stop := false
	for !stop {
		select {
		case <-ticker.C:
			submitted += 1
			select {
			case workC <- true:
				// message sent
			default:
				// message dropped
				dropped += 1
			}
		case <-stopWorkT.C:
			stop = true
		}
	}

	close(workC)

	log.Println("======================")

	for i := 0; i < readWorkers+writeWorkers; i++ {
		result := <-resC
		log.Printf("%+v", result)
	}
	close(resC)

	log.Printf("Time taken: %s", time.Now().Sub(start))
	log.Printf("Total submitted work: %d", submitted)
	log.Printf("Total dropped work:   %d", dropped)
}

// Gatekeeper mediates requests to a Shard.
type Gatekeeper interface {
	Get(req *GetRequest)
	Set(req *SetRequest)
}

// clientResult stores statistics for a client run.
type clientResult struct {
	clientId, total, statusOK, statusNotFound, statusCreated, statusConflict, statusUnknown int64
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

	r := clientResult{clientId: int64(clientId)}

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

	r := clientResult{clientId: int64(clientId)}

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
