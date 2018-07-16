# CHANGELOG

## 0.5.0 (Work in Progress)

* The Cluster Operator now manages RBAC resource for managed resources:
    * `ServiceAccount` and `ClusterRoleBindings` for Kafka pods
    * `ServiceAccount` and `RoleBindings` for the Topic Operator pods
* Renaming of Kubernetes services (Backwards incompatible!)
  * Kubernetes services for Kafka, Kafka Connect and Zookeeper have been renamed to better correspond to their purpose
  * `xxx-kafka` -> `xxx-kafka-bootstrap`
  * `xxx-kafka-headless` -> `xxx-kafka-brokers`
  * `xxx-zookeeper` -> `xxx-zookeeper-client`
  * `xxx-zookeeper-headless` -> `xxx-zookeeper-nodes`
  * `xxx-connect` -> `xxx-connect-api`
* Cluster Operator moving to Custom Resources instead of Config Maps
* TLS support has been added to Kafka, Zookeeper and Topic Operator. The current channels are now encrypted.
    * Zookeeper nodes (ports 2888 and 3888)
    * Kafka brokers and Zookeeper nodes (port 2181)
    * Topic Operator and Kafka brokers (port 9091) and Zookeeper nodes (port 2181)
* Logging configuration for Kafka, Kafka Connect and Zookeeper
* Add support for Pod Affinity and Anti-Affinity
* Configuring different JVM options
* Support for broker rack in Kafka

## 0.4.0

* Better configurability of Kafka, Kafka Connect, Zookeeper
* Support for Kubernetes request and limits
* Support for JVM memory configuration of all components
* Controllers renamed to operators
* Improved log verbosity of Cluster Operator
* Update to Kafka 1.1.0
