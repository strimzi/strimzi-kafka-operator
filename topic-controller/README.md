# Topic Controller

The role of the topic controller is to keep in-sync a set of K8s ConfigMaps describing Kafka topics, 
and those Kafka topics. 

Specifically:
 
* if a config map is created, the controller will create the topic it describes
* if a config map is deleted, the controller will delete the topic it describes
* if a config map is change, the controller will update the topic it describes

And also in the other direction:

* if a topic is created, the controller will create a config map describing it
* if a topic is deleted, the controller will create the config map describing it
* if a topic is changed, the controller will update the config map describing it

## Format of the ConfigMap

By default, the controller only considers ConfigMaps having the label `strimzi.io/kind=topic`, 
but this is configurable via the `TC_CM_LABELS` environment variable.

The `data` of such ConfigMaps supports the following keys:

* `name` The name of the topic. If this is absent the name of the ConfigMap itself is used.
* `partitions` The number of partitions of the Kafka topic. This can be increased, but not decreased.
* `replicas` The number of replicas of the Kafka topic. 
* `config` A string in JSON format representing the topic configuration. 

## Reconciliation

A fundamental problem that the controller has to solve is that there is no 
single source of truth: 
Both the ConfigMap and the topic can be modified independently of the controller. 
Complicating this, the topic controller might not always be able to observe
changes at each in real time (the controller might be down etc).
 
To resolve this, the controller maintains its own private copy of the 
information about each topic. 
When a change happens either in the Kafka cluster, or 
in Kubernetes/OpenShift, it looks at both the state of the other system, and at its 
private copy in order to determine what needs to change to keep everything in sync.  
The same thing happens whenever the controller starts, and periodically while its running.

For example, suppose the topic controller is not running, and a ConfigMap "my-topic" gets created. 
When the controller starts it will lack a private copy of "my-topic", 
so it can infer that the ConfigMap has been created since it was last running. 
The controller will create the topic corresponding to "my-topic" and also store a private copy of the 
metadata for "my-topic".

The private copy allows the controller to cope with scenarios where the topic 
config gets changed both in Kafka and in Kubernetes/OpenShift, so long as the 
changes are not incompatible (e.g. both changing the same topic config key, but to 
different values). 
In the case of incompatible changes, the Kafka configuration wins, and the ConfigMap will 
be updated to reflect that.


## Controller configuration

The controller is configured via a ConfigMap within the K8s/OpenShift cluster.

The data of this ConfigMap supports the following keys:

* `kubernetesMasterUrl`, default: `https://localhost:8443`
* `kafkaBootstrapServers`, default:`localhost:9092`
* `zookeeperConnect`, default: `localhost:2181`
* `zookeeperSessionTimeout` a durection for the timeout on the zookeeper session, default: `2 seconds`
* `fullReconciliationInterval` a duration for the time between full reconciliations, default: `15 minutes`
* `reassignThrottle`, the throttle to use when topic updates require topic partition reassignment
* `reassignVerifyInterval` a duration between executions of the `--verify` stage of partition reassignment, default: `2 minutes`  

The controller watches for changes to the config map and reconfigures itself according.


## Controller environment

* `TC_CM_LABELS` 
– The Kubernetes label selector used to identify ConfigMaps to be managed by the controller.  
* `TC_ZK_SESSION_TIMEOUT`
– The Zookeeper session timeout. For example `10 seconds`. Default: `2 seconds`.
* `TC_KF_BOOTSTRAP_SERVERS`
– The list of Kafka bootstrap servers. Default: `${KAFKA_SERVICE_HOST}:${KAFKA_SERVICE_PORT}` 
* `TC_ZK_CONNECT`
– The Zookeeper connection information. Default: `${KAFKA_ZOOKEEPER_SERVICE_HOST}:${KAFKA_ZOOKEEPER_SERVICE_PORT}`.
* `TC_PERIODIC_INTERVAL`
– The interval between periodic reconciliations.

