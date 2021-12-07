# Kinesis Butler

A task-based gRPC server that subscribes to Kinesis Data Streams. Built with [Tonic](https://github.com/hyperium/tonic) on the [Tokio](https://github.com/tokio-rs/tokio) runtime.

![diagram](/diagram.png)

## Overview

Managing subscriptions across multiple shards, handling I/O failures, and checkpointing processed records are common tasks when using Kinesis Data Streams. However, writing robust software to handle these complexities can be challenging. Moreover, if you want to write record processors in different programming languages, porting implementations can be time-consuming and tedious.

Kinesis Butler decouples record consumption from record processing, enabling developers to easily consume data from [Amazon Kinesis](http://aws.amazon.com/kinesis) while giving them the flexibility to write record processing logic in a variety of [supported languages](https://grpc.io/docs/languages/).

## Features

- Supports multiple storage backends for maintaining shard leases and record checkpoints
- Dynamically redistributes shard leases across available connections
- Simple API for streaming/checkpointing records

## Getting Started

To run Kinesis Butler, you'll need valid AWS credentials and a running instance of a supported storage backend.

Once the service is running, a client can send an `Initialize` proto request to the service. The request data consists of the streams to initialize. Upon receiving the request, Kinesis Butler will get the necessary shard information for each of the specified streams and create/update leases. `Initialize` requests are only necessary before a client attempts to stream data from a new stream.

To stream records, a record processing application sends `GetRecords` request and specifies the streams it wants to receive data from. It will start to receive data from different shards in the stream. It is up to the client to `Checkpoint` records, as Kinesis Butler will start reading from a shard after its last checkpointed sequence number.

## Scaling

Record Processors can seamlessly scale horizontally. Upon receiving new connections that read from the same stream, Kinesis Butler will dynamically redistribute the workload across the available connections. For example, if there are 5 record processing applications reading from a stream that contains 250 shards, each record processor will eventually receive data from a maximum of 50 shards. If a connection is dropped, the workload is redistributed after some time.
