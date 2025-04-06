I aimed to build a fully functional, linearizable distributed key-value store using the Raft consensus algorithm. To achieve this, I first implemented Raft’s core functionality and then added a client-side deduplication mechanism to make it a fully linearizable storage. I have both in-memory and disk storage implementations for the Raft log, state, and state machine to allow efficient end-to-end testing. Below are the detailed components of my distributed key-value store.

The store is built in Java using Spring. Persistent states and logs are stored on disk with append-only, file-backed logs, while the state machine is persisted using an embedded H2 database. Internal RPCs and client interactions are handled via RESTful HTTP APIs.

Leader Election

Log Replication

Safety

Linearizable Reads

Log Entry Persistence via Binary Log

Client-Side Deduplication

Extensions: Log Compaction via Snapshots, membership changes, transactions where multiple keys can be changed simultaneously.

Article: "Why Distributed Systems Are Hard: Lessons Learned from Building a Raft-Based Distributed Key-Value Store"

Usage:
To start a three-node cluster:
