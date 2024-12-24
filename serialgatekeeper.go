package main

import (
	"sync"

	"github.com/theodesp/blockingQueues"
)

// SerialGatekeeper implements the current CouchDB gatekeeping:
// all requests to a shard are serialised in a single stream in
// the order that they arrive.
type SerialGatekeeper struct {
	queue *blockingQueues.BlockingQueue // GetRequest or SetRequest
	m     sync.Mutex
	s     *Shard
}

// Run should be called in a separate Go routine
func (g *SerialGatekeeper) Run() {
	for {
		res, _ := g.queue.Get()
		switch res.(type) {
		case *GetRequest:
			g.s.Get(res.(*GetRequest))
		case *SetRequest:
			g.s.Set(res.(*SetRequest))
		default:
			panic("unexpected queued item")
		}
	}
}

func (g *SerialGatekeeper) Get(req *GetRequest) {
	g.queue.Put(req)
}
func (g *SerialGatekeeper) Set(req *SetRequest) {
	g.queue.Put(req)
}
