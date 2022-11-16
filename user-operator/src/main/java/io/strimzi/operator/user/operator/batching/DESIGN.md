# Micro-batching for Kafka Admin API requests

This package contains the tooling for micro-batching of Kafka Admin API requests to get better performance in the User Operator while not overloading the Kafka brokers.
It consists of an abstract class `AbstractBatchReconciler` and several implementations for handling different APIs:
* Altering SCRAM-SHS credentials (handles both deletion and change of passwords)
* Altering quotas (quotas deletion happens when you alter the existing quotas to `null` quotas)
* Adding new ACL rules
* Deleting existing ACL rules

The micro-batching reconcilers collect the Kafka Admin API requests into a _queue_.
When the amount of requests in the queue reaches some pre-configured number (called batch size or block size), the reconciler will trigger the batch.
Similarly, when the request is waiting in the queue for pre-configured amount of time (called batch time), the reconciler will trigger the batch as well.
The actual setting can be for example batch size of 100 requests and batch time of 100ms.
Whatever is reached first will trigger the enqueued request to be sent to Kafka.

This allows us to control the throughput as well as the overall latency of the requests and tune between them.
Increasing the block time will give the reconciler more time to collect the events to batch them more efficiently.
But at the cost of increased latency - time for which the request will sit in the queue.
Decreasing the batch time will allow the requests to be sent faster.
But fewer requests will collect in the queue, so the requests will be smaller.

The _request_ queued into the reconciler queue contains several parts:
* Name of the user to which this request belongs
* The actual request (e.g. list of ACL rules to be added)
* `CompletableFuture` to inform the object which enqueued the request about the result

The `AbstractBatchReconciler` provides the basic methods for the micro-batching to work:
* Queue for queueing of the requests
* A countdown latch mechanism to trigger the batch of requests when either the block size is reached or after the block time has passed
* It has its own thread to be able to trigger the requests to Kafka independently

The different implementations in this package provide their own `reconcile` method.
This method is responsible for:
* Using the Kafka Admin API to send the batch of requests
* To decode the results and use the `CompletableFuture` to inform the _requestors_ about them

While sending the request is very similar for all implementations, the handling of results is not.
The requests can and in different way:
* All requests succeed
* The whole batch fails
* Only some parts of the batch fail

Decoding this is different for each reconciler.
For example, all quotas are part of single request. So the quotas for user `my-user` are a single item of the batch.
But different ACL rules for a single user are independent items of the batch.
So when a user has 10 different ACL rules which should be created, 9 of them might succeed and one might fail.
So the `reconcile` method has to decode these differently and collect all the results for given user because in the User Operator, these would be part of a single request.