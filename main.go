package main

import (
	"log"
	"sync"
	"time"
)

func client(backend *Backend, workC chan bool, clientId int, wg *sync.WaitGroup) {
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
			backend.Get(GetRequest{id: "foo"})
		}
	}
}
func main() {

	var jobsPerSecond float64 = 500
	seconds := 2

	tickerSleep := time.Duration(float64(1*time.Second) / jobsPerSecond)

	// This needs to be (workerSleep / tickerSleep) + 1
	// to avoid drops.
	workers := 1000

	log.Printf("seconds:       %d", seconds)
	log.Printf("jobsPerSecond: %f", jobsPerSecond)
	log.Printf("(total jobs    %d)", seconds*int(jobsPerSecond))
	log.Printf("tickerSleep:   %s", tickerSleep)
	log.Printf("workers:       %d", workers)

	backend := &Backend{
		store: make(map[string]int64),
	}
	workC := make(chan bool)
	wg := sync.WaitGroup{}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go client(backend, workC, i, &wg)
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
type Backend struct {
	// We assume that the get/set in the map is fast enough we can just use a naive lock
	// given we have a millisecond sleep in there.
	m sync.Mutex

	// Don't need a value, so just id -> rev
	store map[string]int64
}

const GetSleep time.Duration = 1 * time.Millisecond
const SetSleep time.Duration = 5 * time.Millisecond

func (b *Backend) Get(req GetRequest) Response {
	time.Sleep(GetSleep)
	b.m.Lock()
	defer b.m.Unlock()
	if rev, ok := b.store[req.id]; ok {
		return Response{
			code: 200,
			id:   req.id,
			rev:  rev,
		}
	} else {
		return Response{
			code: 404,
		}
	}
}
func (b *Backend) Set(req SetRequest) Response {
	time.Sleep(SetSleep)
	b.m.Lock()
	defer b.m.Unlock()
	if rev, ok := b.store[req.id]; ok {
		if req.rev == rev {
			// make a new rev
			newRev := rev + 1
			b.store[req.id] = newRev
			return Response{
				code: 200,
				id:   req.id,
				rev:  newRev,
			}
		} else {
			return Response{
				code: 409,
			}

		}
	} else {
		return Response{
			code: 404,
		}
	}

}
