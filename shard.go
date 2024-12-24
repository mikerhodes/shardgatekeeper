package main

import (
	"net/http"
	"sync"
	"time"
)

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
func (b *Shard) Set(req *SetRequest) {
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
