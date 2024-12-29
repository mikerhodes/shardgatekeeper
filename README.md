# Shard Gatekeeper Simulation

Currently, CouchDB serializes writes to shards. This repository holds a
simulation I'm trying to write to understand the effects of different strategies
for writing shards (eg, parallel reader and writer queues), and the pathological
symptoms that they might cause.

## Modeling

### Shards

These are modeled as a map, of ID to rev. Rev is modeled as an int.

To model latency, a `time.Sleep` is added before the map is modified. The map is
protected by a mutex. Given the `time.Sleep` of at least a millisecond, I
consider the mutex overhead worthwhile to ensure that modification to the shard
will never leave things in an inconsistent state and nor will reads see an
inconsistent state.

Measurements indicate that allowing as many goroutines as possible to call
Get/Set on a Shard concurrently will result in over 200,000 operations a second,
whatever the read/write balance is. At least on my laptop. Given that we'd not
expect more than a thousand read/writes on a given shard in practice, this
theoretical throughput limit is good enough. So the mutex approach seems fine.

### Gatekeepers

The different strategies for allowing work into the Shard are called
gatekeepers, as they gate keep the shard. There are several:

1. `serial` - this to be the same as today's CouchDB where reads/writes are
   serialized via the mailbox for `couch_file` (and perhaps other places). In
   this codebase, I use a very large (10 million) item queue to mock out the
   infinite(ish) mailbox in Erlang.
2. `parallel` - this is a thought-experiment change where reads and writes are
   serialized via a read queue and a write queue. In CouchDB this would be
   introducing two processes per shard file, perhaps above the btree level. Each
   process handles either reads or writes from the shard, and their mailboxes
   are the queues. I mock this using two goroutines and two 10 million item
   queues (one read queue and one write queue).

One could imagine other gatekeepers:

- Capped queues rather than effectively infinite ones. Send 429 when a queue is
  full, ie, load shed it.
- Use a token bucket to limit reads/writes to a certain rate. Again, 429 when no
  tokens (or you'd have to wait too long).
- Probably something fancy where you combine a token bucket with capped queues,
  or at least you reduce the rate work is accepted as the queues get larger.
- Something where you timestamp each request, and drop those over a certain age.

Broadly, the ideal is that you don't have infinite queues. These can first
consume a lot of memory and second cause you to do too much work because you
never load shed work that the client has likely given up on anyway.

Also, you want to have retain throughput even in the face of increasing latency
for each operation. While writes have to be serialized to maintain CouchDB's
invariants (at least without major changes to the file format and/or code),
reads should be easier to parallelise.
