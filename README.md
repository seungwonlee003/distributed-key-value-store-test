I want to focus on the Raft core first and implement the key-value storage on top of it. I wanted to ensure linearizable reads by clients, and to achieve this, 
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
