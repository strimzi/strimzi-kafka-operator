# KRaft examples

The examples in this directory demonstrate how you can use Kraft (ZooKeeper-less Apache Kafka) with Strimzi.
* The [`kafka.yaml`](kafka.yaml) deploys a Kafka cluster with one pool of _KRaft controller_ nodes and one pool of _KRaft broker_ nodes.
* The [`kafka-ephemeral.yaml`](kafka-ephemeral.yaml) deploys a Kafka cluster with one pool of _KRaft controller_ nodes, one pool of _KRaft broker_ nodes and ephemeral storage.
* The [`kafka-with-dual-role-nodes.yaml`](kafka-with-dual-role-nodes.yaml) deploys a Kafka cluster with one pool of KRaft nodes that share the _broker_ and _controller_ roles.

To use KRaft, ensure that the `KafkaNodePools` and `UseKRaft` feature gates are not disabled.
Please note that ZooKeeper-less Apache Kafka is still under development and lacks some critical features.
For instance, JBOD storage is not supported (you can specify `type: jbod` storage in Strimzi custom resources, but it should contain only a single volume).
