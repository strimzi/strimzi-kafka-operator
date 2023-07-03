# User Controller and WebServer

## `UserController`

Is the main controller for the Strimzi User Operator.
It collects events from the `KafkaUser` resources and form `Secrets` with the `strimzi.io/kind=KafkaUser` label.
It handles these events and enqueues them for reconciliation by the `UserControllerLoop`.
It also handles part of the metrics such as custom resource counts.
It is using a pool of controller loop threads to reconcile the users in parallel.

## `UserControllerLoop`

`UserControllerLoop` extends the `AbstractControllerLoop` and adds the logic for reconciling users.
The core of the reconciliation is handled in `KafkaUserOperator` and the operators dedicated for ACLs, Quotas and SCRAM-SHA credentials.
`UserControllerLoop` handles the result and updates the status of the `KafkaUSer` custom resource.
Normally, many `UserControllerLoop` instances - each with its own thread - run in parallel to give the User Operator better scalability.

## `HealthCheckAndMetricsServer`

`HealthCheckAndMetricsServer` is the web server which handles the health checks and Prometheus metrics of the Strimzi User Operator.
It is based on the Jetty webserver which it embeds.
It listens on port 8081 and has following endpoints:
* `/ready` => Readiness check
* `/healthy` => Liveness check
* `/metrics` => Prometheus metrics

The liveness and readiness checks are done based on the `isAlive` and `isReady` methods of the `UserController`.
`UserController` itself decides based on the state of the `USerOperatorLoop` threads.

The metrics are handled from the Micrometer metrics and its Prometheus registry.

### Important design decisions

The handlers for the different endpoints were originally based on `HttpServlet` classes.
But this class implements `Serializable` and was causing issues when the servlet handlers were using non-serializable classes such as the `UserController` or `PrometheusMeterRegistry`.
So instead, the code now uses the Jetty `AbstractHandler` which does not implement `Serializable` and does not have this kind od issues.

Another issue was that Jetty by default redirects the path without trailing `/` to the path with trailing `/`.
So for example `/metrics` was always redirected to `/metrics/`.
To avoid that unnecessary forward, we have to allow the _noll path info_: `setAllowNullPathInfo(true)`.