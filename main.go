package main

import (
	"log"
	"net/http"
	"sync"
	"time"
)

type clientResult struct {
	clientId, total, statusOK, statusNotFound, statusAccepted, statusConflict, statusUnknown int64
}

func readClient(shard Gatekeeper, workC chan bool, resC chan clientResult, clientId int, wg *sync.WaitGroup) {
	defer wg.Done()
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
			log.Println(resp)

			r.total += 1
			switch resp.code {
			case http.StatusOK:
				r.statusOK += 1
			case http.StatusNotFound:
				r.statusNotFound += 1
			case http.StatusCreated:
				r.statusAccepted += 1
			case http.StatusConflict:
				r.statusAccepted += 1
			default:
				r.statusUnknown += 1
			}
		}
	}
	resC <- r
}
func writeClient(shard Gatekeeper, workC chan bool, resC chan clientResult, clientId int, wg *sync.WaitGroup) {
	defer wg.Done()
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

			set := SetRequest{
				id:    "foo",
				rev:   revId,
				respC: make(chan Response, 1),
			}
			shard.Set(set)
			log.Println(<-set.respC)

			// For now only record the status of writes
			r.total += 1
			switch resp.code {
			case http.StatusOK:
				r.statusOK += 1
			case http.StatusNotFound:
				r.statusNotFound += 1
			case http.StatusCreated:
				r.statusAccepted += 1
			case http.StatusConflict:
				r.statusAccepted += 1
			default:
				r.statusUnknown += 1
			}
		}
	}
	resC <- r
}
func main() {

	// TODO now we have a backend, need to make the intermediary work mediation thing
	// that we can swap around. Would need to wrap around the backend.
	// Maybe the Get/Set on the Shard is the right interface --- extract it, and implement
	// it via the Get/Set strategies.

	var jobsPerSecond float64 = 500
	seconds := 2

	tickerSleep := time.Duration(float64(1*time.Second) / jobsPerSecond)

	// This needs to be (workerSleep / tickerSleep) + 1
	// to avoid drops.
	readWorkers := 2
	writeWorkers := 2

	log.Printf("seconds:       %d", seconds)
	log.Printf("jobsPerSecond: %f", jobsPerSecond)
	log.Printf("(total jobs    %d)", seconds*int(jobsPerSecond))
	log.Printf("tickerSleep:   %s", tickerSleep)
	log.Printf("read workers:  %d", readWorkers)
	log.Printf("write workers: %d", writeWorkers)

	shard := &Shard{
		store: make(map[string]int64),
	}
	workC := make(chan bool)
	resC := make(chan clientResult, readWorkers+writeWorkers)
	wg := sync.WaitGroup{}

	clientId := 0
	for i := 0; i < readWorkers; i++ {
		wg.Add(1)
		clientId += 1
		go readClient(shard, workC, resC, clientId, &wg)
	}
	for i := 0; i < writeWorkers; i++ {
		wg.Add(1)
		clientId += 1
		go writeClient(shard, workC, resC, clientId, &wg)
	}

	ticker := time.NewTicker(tickerSleep)
	defer ticker.Stop()

	stopWorkT := time.NewTimer(2 * time.Second)

	dropped := 0
	submitted := 0

	start := time.Now()

	stop := false
	for !stop {
		select {
		case <-ticker.C:
			submitted += 1
			// log.Println("sending work")
			// log.Println("Current time: ", t)
			select {
			case workC <- true:
				// message sent
				// log.Println("sent work")
			default:
				// message dropped
				dropped += 1
			}
		case <-stopWorkT.C:
			stop = true
		}
	}

	close(workC)

	wg.Wait()

	for i := 0; i < readWorkers+writeWorkers; i++ {
		result := <-resC
		log.Printf("%+v", result)
	}
	close(resC)

	log.Println("======================")
	log.Printf("Time taken: %s", time.Now().Sub(start))
	log.Printf("Total submitted work: %d", submitted)
	log.Printf("Total dropped work:   %d", dropped)
}

type Gatekeeper interface {
	Get(req *GetRequest)
	Set(req SetRequest)
}

type GetRequest struct {
	id    string
	respC chan Response
}
type SetRequest struct {
	id    string
	rev   int64
	respC chan Response
}
type Response struct {
	code int
	id   string
	rev  int64
}
type Shard struct {
	// We assume that the get/set in the map is fast enough we can just use a naive lock
	// given we have a millisecond sleep in there.
	m sync.Mutex

	// Don't need a value, so just id -> rev
	store map[string]int64
}

// GetSleep and SetSleep allow us to control the latency of operations.
const GetSleep time.Duration = 1 * time.Millisecond
const SetSleep time.Duration = 5 * time.Millisecond

// Get retrieves a document by ID, sending the response on req.respC.
func (b *Shard) Get(req *GetRequest) {
	time.Sleep(GetSleep)
	b.m.Lock()
	defer b.m.Unlock()
	if rev, ok := b.store[req.id]; ok {
		req.respC <- Response{
			code: http.StatusOK,
			id:   req.id,
			rev:  rev,
		}
	} else {
		req.respC <- Response{
			code: http.StatusNotFound,
		}
	}
}

// Set creates or updates a document by ID, sending the response on req.respC.
// To create a document, set rev to 0.
func (b *Shard) Set(req SetRequest) {
	time.Sleep(SetSleep)
	b.m.Lock()
	defer b.m.Unlock()
	currRev, found := b.store[req.id]

	if (found && req.rev == currRev) || (!found && req.rev == 0) {
		// Write accepted for existing doc
		// Write accepted for new doc if rev 0
		// make a new rev
		newRev := req.rev + 1
		b.store[req.id] = newRev
		req.respC <- Response{
			code: http.StatusCreated,
			id:   req.id,
			rev:  newRev,
		}
	} else if found && req.rev != currRev {
		// Write not accepted for existing doc
		req.respC <- Response{
			code: http.StatusConflict,
		}
	} else {
		// Doc not found, and rev not 0.
		req.respC <- Response{
			code: http.StatusNotFound,
		}
	}

}
