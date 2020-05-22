
# CHANGELOG

## 0.18.0

* Add possibility to set Java System Properties for User Operator and Topic Operator via `Kafka` CR.
* Make it possible to configure PodManagementPolicy for StatefulSets
* Update build system to use `yq` version 3 (https://github.com/mikefarah/yq)
* Add more metrics to Cluster and User Operators
* New Grafana dashboard for Operator monitoring 
* Allow `ssl.cipher.suites`, `ssl.protocol` and `ssl.enabled.protocols` to be configurable for Kafka and the different components supported by Strimzi
* Add support for user configurable SecurityContext for each Strimzi container
* Allow standalone User Operator to modify status on KafkaUser
* Add support for Kafka 2.4.1
* Add support for Kafka 2.5.0
* Remove TLS sidecars from ZooKeeper pods, using native ZooKeeper TLS support instead
* Add metrics for Topic Operator
* Use Strimzi Kafka Bridge 0.16.0
* Add support for CORS in the HTTP Kafka Bridge
* Pass HTTP Proxy configuration from operator to operands
* Add Cruise Control support, KafkaRebalance resource and rebalance operator

## 0.17.0

* Add possibility to set Java System Properties via CR yaml
* Add support for Mirror Maker 2.0
* Add Jmxtrans deployment
* Add public keys of TLS listeners to the status section of the Kafka CR
* Add support for using a Kafka authorizer backed by Keycloak Authorization Services

## 0.16.0

* Add support for Kafka 2.4.0 and upgrade from Zookeeper 3.4.x to 3.5.x
* Drop support for Kafka 2.2.1 and 2.3.0
* Add KafkaConnector resource and connector operator
* Let user choose which node address will be used as advertised host (`ExternalDNS`, `ExternalIP`, `InternalDNS`, `InternalIP` or `Hostname`)
* Add support for tini
* When not explicitly configured by the user in `jvmOptions`, `-Xmx` option is calculated from memory requests rather than from memory limits
* Expose JMX port on Kafka brokers via an internal service
* Add support for `externalTrafficPolicy` and `loadBalancerSourceRanges` properties on loadbalancer and nodeport type services
* Add support for user quotas
* Add support for Istio protocol selection in service port names  
Note: Strimzi is essentially adding a `tcp-` prefix to the port names in Kafka services and headless services.  
(e.g clientstls -> tcp-clientstls)
* Add service discovery labels and annotations
* Add possibility to specify custom server certificates to TLS based listeners

## 0.15.0

* Drop support for Kafka 2.1.0, 2.1.1, and 2.2.0
* Add support for Kafka 2.3.1
* Improved Kafka rolling update
* Improve Kafka Exporter Grafana dashboard
* Add sizeLimit option to ephemeral storage (#1505)
* Add `schedulerName` to `podTemplate` (#2114)
* Allow overriding the auto-detected Kubernetes version
* Garbage Collection (GC) logging disabled by default
* Providing PKCS12 truststore and password in the cluster and clients CA certificates Secrets
* Providing PKCS12 keystore and password in the TLS based KafkaUser related Secret

## 0.14.0

* Add support for configuring Ingress class (#1716)
* Add support for setting custom environment variables in all containers
* Add liveness and readiness checks to Mirror Maker
* Allow configuring loadBalancerIP for LoadBalancer type services
* Allow setting labels and annotations for Persistent Volume Claims
* Add support for Jaeger tracing in Kafka Mirror Maker and Kafka Connect
* Add support for deploying Kafka Exporter
* Add initial support for OAuth authentication

## 0.13.0

* Allow users to manually configure ACL rules (for example, using `kafka-acls.sh`) for special Kafka users `*` and `ANONYMOUS` without them being deleted by the User Operator
* Add support for configuring a Priority Class name for Pods deployed by Strimzi
* Add support for Kafka 2.3.0
* Add support for Kafka User resource status
* Add support for Kafka Connect resource status
* Add support for Kafka Connect S2I resource status
* Add support for Kafka Bridge resource status
* Add support for Kafka Mirror Maker resource status
* Add support for DNS annotations to `nodeport` type external listeners

## 0.12.0

* **Drop support for Kubernetes 1.9 and 1.10 and OpenShift 3.9 and 3.10.**
**Versions supported since Strimzi 0.12.0 are Kubernetes 1.11 and higher and OpenShift 3.11 and higher.** 
**This was required because the CRD versioning and CRD subresources support.** 
* Added support for Kafka 2.2.0 and 2.1.1, dropped support for Kafka 2.0.0 and 2.0.1
* Persistent storage improvements
  * Add resizing of persistent volumes
  * Allow to specify different storage class for every broker
  * Adding and removing volumes in Jbod Storage
* Custom Resources improvements
  * New CRD version `v1beta1`. See documentation for more information about upgrading from `v1alpha1` to `v1beta1`.
  * Log at the warn level when a custom resource uses deprecated or unknown properties
  * Add initial support for the `status` sub-resource in the `Kafka` custom resource 
* Add support for [Strimzi Kafka Bridge](https://github.com/strimzi/strimzi-kafka-bridge) for HTTP protocol
* Reduce the number of container images needed to run Strimzi to just two: `kafka` and `operator`.
* Add support for unprivileged users to install the operator with Helm
* Support experimental off-cluster access using Kubernetes Nginx Ingress
* Add ability to configure Image Pull Secrets for all pods in Cluster Operator
* Support for SASL PLAIN mechanism in Kafka Connect and Mirror Maker (for use with non-Strimzi Kafka cluster)

## 0.11.0

* Add support for JBOD storage for Kafka brokers
* Allow users to configure the default ImagePullPolicy
* Add Prometheus alerting
    * Resources for alert manager deployment and configuration
    * Alerting rules with alert examples from Kafka and Zookeeper metrics
* Enrich configuration options for off cluster access
* Support for watching all namespaces
* Operator Lifecycle Manager integration

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
