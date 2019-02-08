# CHANGELOG

## 0.11.0

* Add support for JBOD storage for Kafka brokers
* Allow users to configure the default ImagePullPolicy

## 0.10.0

* Support for Kafka 2.1.0
* Support for Kafka upgrades
* Add healthchecks to TLS sidecars
* Add support for new fields in the Pod template: terminationGracePeriod, securityContext and imagePullSecrets
* Rename annotations to use the `strimzi.io` domain consistently (The old annotations are deprecated, but still functional):
    * `cluster.operator.strimzi.io/delete-claim` → `strimzi.io/delete-claim` 
    * `operator.strimzi.io/manual-rolling-update` → `strimzi.io/manual-rolling-update` 
    * `operator.strimzi.io/delete-pod-and-pvc` → `strimzi.io/delete-pod-and-pvc`
    * `operator.strimzi.io/generation` → `strimzi.io/generation`
* Add support for mounting Secrets and Config Maps into Kafka Connect and Kafka Connect S2I
* Add support for NetworkPolicy peers in listener configurations
* Make sure the TLS sidecar pods shutdown only after the main container
* Add support for Pod Disruption Budgets

## 0.9.0

* Add possibility to label and annotate different resources (#1039)
* Add support for TransactionalID in KafkaUser resource
* Update to Kafka 2.0.1
* Add maintenance time windows support for allowing CA certificates renewal rolling update started only in specific times (#1117)  
* Add support for upgrading between Kafka versions (#1103). This removes support for `STRIMZI_DEFAULT_KAFKA_IMAGE` environment variable in the Cluster Operator, replacing it with `STRIMZI_KAFKA_IMAGES`.  


## 0.8.2

* Run images under group 0 to avoid storage issues

## 0.8.1

* Fix certificate renewal issues

## 0.8.0

* Support for unencrypted connections on LoadBalancers and NodePorts.
* Better support for TLS hostname verification for external connections
* Certificate renewal / expiration
* Mirror Maker operator
* Triggering rolling update / pod deletion manually

## 0.7.0

* Exposing Kafka to the outside using:
  * OpenShift Routes
  * LoadBalancers
  * NodePorts
* Use less wide RBAC permissions (`ClusterRoleBindings` where converted to `RoleBindings` where possible)
* Support for SASL authentication using the SCRAM-SHA-512 mechanism added to Kafka Connect and Kafka Connect with S2I support 
* Network policies for managing access to Zookeeper ports and Kafka replication ports
* Use OwnerReference and Kubernetes garbage collection feature to delete resources and to track the ownership

## 0.6.0

* Helm chart for Strimzi Cluster Operator
* Topic Operator moving to Custom Resources instead of Config Maps
* Make it possible to enabled and disable:
  * Listeners
  * Authorization
  * Authentication
* Configure Kafka _super users_ (`super.users` field in Kafka configuration)
* User Operator
  * Managing users and their ACL rights
* Added new Entity Operator for deploying:
  * User Operator
  * Topic Operator
* Deploying the Topic Operator outside of the new Entity Operator is now deprecated
* Kafka 2.0.0
* Kafka Connect:
  * Added TLS support for connecting to the Kafka cluster
  * Added TLS client authentication when connecting to the Kafka cluster 

## 0.5.0

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
* TLS support has been added to Kafka, Zookeeper and Topic Operator. The following channels are now encrypted:
    * Zookeeper cluster communication
    * Kafka cluster commbunication
    * Communication between Kafka and Zookeeper
    * Communication between Topic Operator and Kafka / Zookeeper
* Logging configuration for Kafka, Kafka Connect and Zookeeper
* Add support for [Pod Affinity and Anti-Affinity](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity)
* Add support for [Tolerations](https://v1-9.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.9/#toleration-v1-core)
* Configuring different JVM options
* Support for broker rack in Kafka

## 0.4.0

* Better configurability of Kafka, Kafka Connect, Zookeeper
* Support for Kubernetes request and limits
* Support for JVM memory configuration of all components
* Controllers renamed to operators
* Improved log verbosity of Cluster Operator
* Update to Kafka 1.1.0
