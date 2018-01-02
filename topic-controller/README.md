# Topic Operator

The role of the topic operator is to keep in-sync a set of K8s ConfigMaps describing Kafka topics, 
and those Kafka topics. 

Specifically:
 
* if a config map is created, the operator will create the topic it describes
* if a config map is deleted, the operator will delete the topic it describes
* if a config map is change, the operator will update the topic it describes

And also in the other direction:

* if a topic is created, the operator will create a config map describing it
* if a topic is deleted, the operator will create the config map describing it
* if a topic is changed, the operator will update the config map describing it

## Format of the ConfigMap

The operator only considers ConfigMaps having all the labels:

* `app=barnabas`
* `kind=topic`

The `data` of such ConfigMaps supports the following keys:

* `name` The name of the topic. If this is absent the name of the ConfigMap itself is used.
* `partitions` The number of partitions of the Kafka topic. This can be increased, but not decreased.
* `replicas` The number of replicas of the Kafka topic. 
* `config` A multiline string in YAML format representing the topic configuration. 

## Reconciliation

The fundamental difficulty that the operator has to solve is that there is no 
single source of truth: Both the ConfigMap and the topic can be modified independently 
of the operator. Complicating this, the topic operator might not always be able to observe
changes at each in real time (the operator might be down etc).
 
To resolve this, the operator maintains its own private copy of the 
information about each topic. When a change happens either in the Kafka cluster, or 
in Kubernetes/OpenShift, it looks at both the state of the other system, and at its 
private copy in order to determine what needs to change to keep everything in sync.  
The same thing happens whenever the operator starts, and periodically while its running.

The private copy allows the operator to usually cope with scenarios where the topic 
config gets changed both in Kafka and in Kubernetes/OpenShift, so long as the 
changes are not incompatible (e.g. both changeing the same topic config key, but to 
different values). 


## Operator configuration

The operator is configured via a ConfigMap within the K8s/OpenShift cluster.

The data of this ConfigMap supports the following keys:

* `kubernetesMasterUrl`, default: `localhost:8443`
* `kafkaBootstrapServers`, default:`localhost:9093`
* `zookeeperConnect`, default: `localhost:2021`
* `zookeeperSessionTimeout` a durection for the timeout on the zookeeper session, default: `2 seconds`
* `fullReconciliationInterval` a duration for the time between full reconciliations, default: `15 minutes`
* `reassignThrottle`, the throttle to use when topic updates require topic partition reassignment
* `reassignVerifyInterval` a duration between executions of the `--verify` stage of partition reassignment, default: `2 minutes`  

The operator watches for changes to the config map and reconfigures itself according.


## Operator command line and environment

* `--master-url` (or the `TOPICOP_K8S_MASTER_URL` env var if absent from the command line) 
  the URL of the master apiserver in which to find the topic operator's ConfigMap. 
* `--config-namespace` (or the `TOPICOP_K8S_CONFIG_NAMESPACE` env var if absent from the 
  command line) the namespace within the master apiserver which contains the topic 
  operator's ConfigMap.
* `--config-cmname` (or the `TOPICOP_K8S_CONFIG_CMNAME` env var if absent from the 
  command line) the name of the config map (with the namespace given by `--config-namespace`)
  which contains the topic 
  operator's ConfigMap.
* `--help` command line help, and exit
* `--help:config` for help on the topic operators configuration. 



## Possible future directions

* Grow an HTTP REST API for changing topics, and representing them as YAML resources, evolving into 
  an aggregated K8s apiserver.