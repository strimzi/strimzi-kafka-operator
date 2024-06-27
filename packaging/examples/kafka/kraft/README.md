# KRaft examples

The examples in this directory demonstrate how you can use Kraft (ZooKeeper-less Apache Kafka) with Strimzi.
* The [`kafka.yaml`](kafka.yaml) deploys a Kafka cluster with one pool of _KRaft controller_ nodes and one pool of _KRaft broker_ nodes.
* The [`kafka-ephemeral.yaml`](kafka-ephemeral.yaml) deploys a Kafka cluster with one pool of _KRaft controller_ nodes, one pool of _KRaft broker_ nodes and ephemeral storage.
* The [`kafka-with-dual-role-nodes.yaml`](kafka-with-dual-role-nodes.yaml) deploys a Kafka cluster with one pool of KRaft nodes that share the _broker_ and _controller_ roles.
* The [`kafka-single-node.yaml`](kafka-single-node.yaml) deploys a Kafka cluster with a single Kafka node that has both _broker_ and _controller_ roles.

Please note that ZooKeeper-less Apache Kafka is still under development and has some limitations.
For instance, scaling of controller node or JBOD storage (you can specify `type: jbod` storage in Strimzi custom resources, but it should contain only a single volume) are not supported.
