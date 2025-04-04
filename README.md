I wanted to make a fully functional and linearizable distributed key-value store. To achieve this, I adopted a Raft consensus algorithm. 
I first implemented the Raft core and I found it neccessary to implement additional linearizable reads and client-side deduplication mechanism to ensure the full linearizability. 
To make distributed key-value store fully functional, I also made durable disk writes for
persistent states such as the persistent variables like votedFor, and others including log entries, and the state machine. Below are the in-depth details of each component of my distributed key-value store.  

leader election

log replication

linearizable reads across all nodes 

log entries persistence via bin log

client side de-duplication

extensions:
log compaction via snapshots

article: why distributed systems are hard
