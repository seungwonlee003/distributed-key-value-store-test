I wanted to make a fully functional and linearizable distributed key-value store. To achieve this, I adopted a Raft consensus algorithm. 
I first implemented the Raft core and I found it neccessary to implement additional linearizable reads and client-side deduplication mechanism to ensure the full linearizability. 
To make distributed key-value store fully functional, I also made durable disk writes for
persistent states such as votedFor, and others including log entries, and the state machine. Below are the in-depth details of each component of my distributed key-value store.  

Distributed key-value store was made with Java with the help of Spring. Persistent states and logs are persisted on disk with using append-only, file-backed logs and the state machine is persisted with embedded H2 database. Internal RPC calls and client's interaction with the database are simulated with RESTful APIs based on HTTP.

leader election

log replication

linearizable reads across all nodes 

log entries persistence via bin log

client side de-duplication

extensions:
log compaction via snapshots

article: why distributed systems are hard - lessons learnt from building raft-based distributed key value store
