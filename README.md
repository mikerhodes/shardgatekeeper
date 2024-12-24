# Shard Gatekeeper Simulation

Currently, CouchDB serializes writes to shards. This repository holds a
simulation I'm trying to write to understand the effects of different strategies
for writing shards (eg, parallel reader and writer queues), and the pathological
symptoms that they might cause.
