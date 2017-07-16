# android-group-messenger

Simple Dynamo

This project is about implementing a simplified version of Dynamo. The three main pieces that are implemented: 1) Partitioning, 2) Replication, and 3) Failure handling. The main goal is to provide both availability and linearizability at the same time. In other words, my implementation always performs read and write operations successfully even under failures. At the same time, a read operation always return the most recent value. The partitioning and replication are done exactly the way Dynamo does.
