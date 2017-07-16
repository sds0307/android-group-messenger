# android-group-messenger

#Simple Messenger

In this project, I wrote a simple messenger app on Android. The goal of this app is simple: enabling two Android devices to send messages to each other

#Group Messenger 1

This project builds on the previous simple messenger and points to the next project. I designed a group messenger that can send message to multiple AVDs and store them in a permanent key-value storage.

#Group Messenger 2

In this project I added ordering guarantees to my group messenger. The guarantees I implemented are total ordering as well as FIFO ordering. I stored all the messages in the content provider. When I store the messages and assign sequence numbers, my mechanism provides total and FIFO ordering guarantees.

#Simple Dht

In this project, I designed a simple DHT based on Chord. Although the design is based on Chord, it is a simplified version of Chord. Three things that are implemented: 1) ID space partitioning/re-partitioning, 2) Ring-based routing, and 3) Node joins.

#Simple Dynamo

This project is about implementing a simplified version of Dynamo. The three main pieces that are implemented: 1) Partitioning, 2) Replication, and 3) Failure handling. The main goal is to provide both availability and linearizability at the same time. In other words, my implementation always performs read and write operations successfully even under failures. At the same time, a read operation always return the most recent value. The partitioning and replication are done exactly the way Dynamo does.
