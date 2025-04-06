Intro

I aimed to build a fully functional, linearizable distributed key-value store using the Raft consensus algorithm. To achieve this, I first implemented Raft’s core functionality and then added a client-side deduplication mechanism to make it a fully linearizable storage. I have both in-memory and disk storage implementations for the Raft log, state, and state machine to allow efficient end-to-end testing. Below are the detailed components of my distributed key-value store.

The store is built in Java using Spring. Persistent states and logs are stored on disk with append-only, file-backed logs, while the state machine is persisted using an embedded H2 database. Internal RPCs and client interactions are handled via RESTful HTTP APIs.

refer to github wiki page for more details on implementation details, trobleshooting, and future extensions. 

Article: "Why Distributed Systems Are Hard: Lessons Learned from Building a Raft-Based Distributed Key-Value Store"


Usage:

discuss how to use this database:
1. edit application.properties file to set up clusters and their addresses
2. 3 endpoints visible: /get, /insert, /update, /delete.
3. include client UUID and sequence number, etc

Manual Deployment
To test locally, you can use a text editor that can edit executable configurations, such as IntelliJ.

Another simple way to test this configuration is to open 4 different terminals and put a different command in each one. For this specific case, you need to be in the servlet/ folder and the commands to be executed are:

Terminal 1:
