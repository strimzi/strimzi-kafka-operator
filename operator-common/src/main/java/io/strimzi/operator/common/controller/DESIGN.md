# Abstract controller

This package contains some of the pieces used to build the User Operator controller.
Creating these separately in the `operator-common` module should allow their re-use.
The expectation is that they can be reused in the future in the `StrimziPodSet` controller and possibly other future controllers.

## `AbstractControllerLoop`

In order to avoid having a too big class with many methods, the _controller_ is split into two parts:
* `Controller` type class is responsible for receiving the events from Kubernetes, handling then and if needed, enqueueing them into the work queue for reconciliation
* `ControllerLoop` type class which is responsible for picking the next event from the queue and reconciles it

The logic in these classes differs significantly and it is mainly the work queue which connects them.
The `Controller` class is dealing with Kubernetes events and deciding to which custom resource they belong and whether they should be acted upon.
For example ... does it match the selector? Is the event interesting to us?
The `ControllerLoop` deals with the reconciliation itself => what action needs to be taken.

The `AbstractControllerLoop` provides basic abstraction for the `ControllerLoop` type classes such as the `UserControllerLoop`.
`AbstractControllerLoop` contains the shared logic for picking the events from the queue and handling some basic metrics and lock management to avoid reconciling the same resource multiple times in parallel.
It also wraps around its own thread which is used to pick the events from the queue and run the reconciliations.
Use of service executors and thread pool executors has been considered.
But it was decided against it because of having an independent queueing mechanism allows us to more easily control the queues and for example decide whether duplicate events should be enqueued in it or not. 

## `ReconciliationLockManager`

When running multiple `ControlLoop` instances in parallel, we need to make sure that a single resource is not reconciled multiple times in parallel since that might cause issues.
`ReconciliationLockManager` is a simple lock manager which can be used to track if a reconciliation for given resource is in progress or not.
It is based on `ConcurrentHashMap` and `ReentrantLock`.
`ConcurrentHashMap` helps with the atomicity of the operations.
The `ReconciliationLockManager` also tracks how many _consumers_ are waiting for given lock and when nobody is waiting, it will proactively remove it from the map to make sure the locks are not staying there long after the resource has been deleted.

## `SimplifiedReconciliation`

`SimplifiedReconciliation` is used by the controllers and the queue to pass around the events.
It identifies the kind, namespace and name of the resource as well as what trigger this event (watch or timer).
Unlike our regular `Reconciliation` it does not have a reconciliation number assigned yet => thanks to that, the sequence of the reconciliation numbers should not have gaps if the event ends up not being enqueued (for example because other event for the same resource is already there).
For this reason it also has its own `equals` implementation to make it easy to compare the events regardless what triggered them.

## `ControllerQueue`

`ControllerQueue` encapsulates a work queue used by the controllers.
It wraps around Java `ArrayBlockingQueue` and provides methods for taking next event from the queue and enqueueing the event.
The `take` method provides the same blocking semantics of the `ArrayBlockingQueue`.
The `enqueue` method allows to enqueue events into the reconciliation queue while making sure each event is queued only once.
The reason for this class and its encapsulation is that it makes it easier to share the queue between the controller and the controller loop while keeping the enqueueing logic and related metrics handling in one place.

## Future work

Currently, there is no `AbstractController` class.
This class might be added in the future as it is more clear which parts will be shared among all the controllers.