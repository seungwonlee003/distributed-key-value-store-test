I aimed to build a fully functional, linearizable distributed key-value store using the Raft consensus algorithm. To achieve this, I first implemented Raft’s core functionality and then added linearizable reads and a client-side deduplication mechanism to ensure full linearizability. I have both in-memory and disk persistence implementations to allow efficient end-to-end testing. Below are the detailed components of my distributed key-value store.

The store is built in Java using Spring. Persistent states and logs are stored on disk with append-only, file-backed logs, while the state machine is persisted using an embedded H2 database. Internal RPCs and client interactions are handled via RESTful HTTP APIs.

Leader Election

Log Replication

Safety

Linearizable Reads Across All Nodes

Log Entry Persistence via Binary Log

Client-Side Deduplication

Extensions: Log Compaction via Snapshots
Article: "Why Distributed Systems Are Hard: Lessons Learned from Building a Raft-Based Distributed Key-Value Store"
