I wanted to make a fully functional distributed key-value storage with a raft-based consensus algorithm. I first implemented the Raft core and decided to ensure linearizable reads by clients at all costs, and to achieve this, 
I found it neccessary to implement additional linearizable reads and client-side deduplication mechanism. To make a functional distributed key-value store, I also made durable disk writes for
persistent states such as the persistent variables such as votedFor, log entries, and the state machine. Below are the in-depth details of each component of my distributed key-value store.  

leader election

log replication

linearizable reads across all nodes 

log entries persistence via bin log

client side de-duplication

extensions:
log compaction via snapshots

article: why distributed systems are hard
