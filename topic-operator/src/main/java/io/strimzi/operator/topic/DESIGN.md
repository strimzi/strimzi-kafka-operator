# Unidirectional topic operator

These notes should be read in conjunction with the [Strimzi proposal][proposal] that covers the unidirectional topic operator (UTO).
While that proposal specifies most of the expected behaviour these notes cover the implementation. 

## Batching

The UTO aims to be scalable in terms of the number of topics that it can operate on.
To that end, the UTO is fairly dumb and uses a similar work queue mechanism to the User Operator, but with some modifications.

In the absence of any changes to any of the watched set of `KafkaTopics` the scalability limit for the UTO is reached when
the controller threads are 100% utilised by processing resyncs. 
Resyncs are usually logical no-ops, that is the topic in Kafka already exists in the correct state, so reconciling the unchanged KafkaTopic doesn't actually change anything within Kafka, or result in an update to the `status`. 
Specifically in the no-op case the controller does the following:

1. Gets the topic metadata (`Admin.describeTopics()`) and topic config (`Admin.describeConfigs()`)
2. Determines that no change is needed.
3. Determines that no update to the `KafkaTopic.status` is needed.

It follows that reducing the cost of those `Admin` operations will allow the steady-state UTO to cope with larger numbers of `KafkaTopic`. 
Kafka provides a useful mechanism for getting higher throughput for metadata operations: batching.

For this reason the UTO tries to make use of the request batching supported by the `Admin` client.
It does this by using a similar heuristic to that used in the Apache Kafka `Producer` client: Trading a bit of latency (in the form of a configurable "linger" duration) to in order to collect a batch of items to achieve higher throughput.
Thus rather than reconciling individual KafkaTopics the UTO reconciles _batches_ of KafkaTopics. 
* The batches are created on each iteration of the `BatchingLoop.LoopRunnable`.
* Once a batch is created, the topic events within it are reconciled together through to completion (1 iteration of `LoopRunnable` => 1 batch => N topic events).
* It is only `Admin` operations that are batched, because Kubernetes' API doesn't support batching.

## Finalizers

Note the use of finalizers can prevent other resources, such as the containing `Namespace` from being deleted.

The items in a batch are not simply `KafkaTopics`, but upserts to, and deletions of, `KafkaTopics`.
This is necessary because the UTO supports two deletion modes: with and without the use of finalizers.
The without-finalizers case means we need to capture the state of the `KafkaTopic` when it is deleted (i.e. at the point the event is being added to the queue), because it might not exist in Kube by the point the event is actually processed.
We need the state of the KafkaTopic, (rather than using `null` to mean the topic was deleted, as other Strimzi operators do) because the reconciliation logic depends on the state of the KafkaTopic even in the deletion case -- The value of the `strimzi.io/managed` annotation affects whether the topic in Kafka should be deleted.

## Concurrent reconciliation

For simplicity the `BatchingLoop.LoopRunnable` prevents two events about the same `KafkaTopic` in the same batch.
Where this rule would be broken, the later events are pushed back onto the head of the queue (that is, it's really a deque) for processing in a later batch.
If there were concurrent controllers processing the queue (currently only a single thread is supported) a mechanism would be required to prevent two events for the same topic being processed concurrently.

## Assumptions

The UTO assumes its Kafka credentials grant it the ability to:
    * describe all topics 
    * describe all topics configs
    * create topics
    * create partitions
    * delete topics
    * list partition reassignments
    * Describe broker configs. This one isn't entirely required, it's used to determine whether to log a warning about `auto.create.topics.enable`.


[proposal]: https://github.com/strimzi/proposals/blob/main/051-unidirectional-topic-operator.md