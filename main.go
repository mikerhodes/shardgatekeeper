package main

import (
	"log"
	"net/http"
	"sync"
	"time"
)

func readClient(shard *Shard, workC chan bool, clientId int, wg *sync.WaitGroup) {
	defer wg.Done()
	// log.Printf("[%d] client started", clientId)
	// defer log.Printf("[%d] client stopped", clientId)

	// Now loop on the channel that tells us to do work
	for workC != nil {
		select {
		case _, ok := <-workC:
			if !ok {
				workC = nil
				break
			}
			// simulate work
			log.Println(shard.Get(GetRequest{id: "foo"}))
		}
	}
}
func writeClient(shard *Shard, workC chan bool, clientId int, wg *sync.WaitGroup) {
	defer wg.Done()
	// log.Printf("[%d] client started", clientId)
	// defer log.Printf("[%d] client stopped", clientId)

	// Now loop on the channel that tells us to do work
	for workC != nil {
		select {
		case _, ok := <-workC:
			if !ok {
				workC = nil
				break
			}
			// simulate work - read/modify/update
			resp := shard.Get(GetRequest{id: "foo"})
			if resp.code == http.StatusNotFound {
				log.Println(shard.Set(SetRequest{id: "foo", rev: 0}))
			} else {
				log.Println(shard.Set(SetRequest{id: "foo", rev: resp.rev}))
			}
		}
	}
}
func main() {

	// TODO now we have a backend, need to make the intermediary work mediation thing
	// that we can swap around. Would need to wrap around the backend.

	var jobsPerSecond float64 = 500
	seconds := 2

	tickerSleep := time.Duration(float64(1*time.Second) / jobsPerSecond)

	// This needs to be (workerSleep / tickerSleep) + 1
	// to avoid drops.
	workers := 2

	log.Printf("seconds:       %d", seconds)
	log.Printf("jobsPerSecond: %f", jobsPerSecond)
	log.Printf("(total jobs    %d)", seconds*int(jobsPerSecond))
	log.Printf("tickerSleep:   %s", tickerSleep)
	log.Printf("workers:       %d", workers)

	shard := &Shard{
		store: make(map[string]int64),
	}
	workC := make(chan bool)
	wg := sync.WaitGroup{}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go readClient(shard, workC, i, &wg)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go writeClient(shard, workC, i, &wg)
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

	log.Println("======================")
	log.Printf("Time taken: %s", time.Now().Sub(start))
	log.Printf("Total submitted work: %d", submitted)
	log.Printf("Total dropped work:   %d", dropped)

}

type GetRequest struct {
	id string
}
type SetRequest struct {
	id  string
	rev int64
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

const GetSleep time.Duration = 1 * time.Millisecond
const SetSleep time.Duration = 5 * time.Millisecond

func (b *Shard) Get(req GetRequest) Response {
	time.Sleep(GetSleep)
	b.m.Lock()
	defer b.m.Unlock()
	if rev, ok := b.store[req.id]; ok {
		return Response{
			code: http.StatusOK,
			id:   req.id,
			rev:  rev,
		}
	} else {
		return Response{
			code: http.StatusNotFound,
		}
	}
}
func (b *Shard) Set(req SetRequest) Response {
	time.Sleep(SetSleep)
	b.m.Lock()
	defer b.m.Unlock()
	rev, ok := b.store[req.id]

	if ok && req.rev == rev {
		// Write accepted for existing doc
		// make a new rev
		newRev := rev + 1
		b.store[req.id] = newRev
		return Response{
			code: http.StatusCreated,
			id:   req.id,
			rev:  newRev,
		}
	} else if !ok && req.rev == 0 {
		// Write accepted for new doc if rev 0
		b.store[req.id] = 1
		return Response{
			code: http.StatusCreated,
			id:   req.id,
			rev:  1,
		}
	} else if ok && req.rev != rev {
		// Write not accepted for existing doc
		return Response{
			code: http.StatusConflict,
		}
	} else {
		// Doc not found, and rev not 0.
		return Response{
			code: http.StatusNotFound,
		}
	}

}
