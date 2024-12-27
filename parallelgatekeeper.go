package main

import (
	"github.com/theodesp/blockingQueues"
)

// ParallelGatekeeper implements parallel execution of serialised
// reads and writes on shards by having a read and a write goroutine.
type parallelGatekeeper struct {
	rQueue, wQueue *blockingQueues.BlockingQueue // GetRequest or SetRequest
	rStopC, wStopC chan bool
	s              *Shard
}

func NewParallelGatekeeper(shard *Shard) *parallelGatekeeper {

	rQueue, _ := blockingQueues.NewArrayBlockingQueue(10_000_000)
	wQueue, _ := blockingQueues.NewArrayBlockingQueue(10_000_000)
	return &parallelGatekeeper{
		rQueue: rQueue,
		wQueue: wQueue,
		s:      shard,
	}
}

func (g *parallelGatekeeper) Start() {
	go g.getRun()
	go g.setRun()
}

// Run should be called in a separate Go routine
func (g *parallelGatekeeper) getRun() {
	for {
		res, _ := g.rQueue.Get()
		g.s.Get(res.(*GetRequest))
	}
}

func (g *parallelGatekeeper) setRun() {
	for {
		res, _ := g.wQueue.Get()
		g.s.Set(res.(*SetRequest))
	}
}

func (g *parallelGatekeeper) Get(req *GetRequest) {
	g.rQueue.Put(req)
}
func (g *parallelGatekeeper) Set(req *SetRequest) {
	g.wQueue.Put(req)
}
