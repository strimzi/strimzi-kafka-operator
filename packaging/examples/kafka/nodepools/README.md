# Kafka Node Pool examples

The examples in this directory show how you can use the Kafka node pool to set up your Kafka cluster.
The Kafka node pools are currently protected by a feature gate `KafkaNodePools` which is in _alpha_ phase and is disabled by default.

_NOTE: Feature gates in the _alpha_ stage should be considered as experimental / in development and should not be used in production._
_Backward incompatible changes might be made to such feature gates or they might be simply removed._

If you want to try out the `KafkaNodePools` feature gate and the examples from this directory, you have to enable the feature gate first.

## Regular examples

The [`kafka.yaml`](./kafka.yaml) example shows a regular Kafka cluster backed by ZooKeeper.
This example file deploys a ZooKeeper cluster with 3 nodes and 2 different pools of Kafka brokers.
Each of the pools has 3 brokers.
The pools in the example use different storage configuration.

## KRaft (ZooKeeper-less) examples

The KRaft examples show an example of a Kafka cluster in the KRaft mode (i.e. without the ZooKeeper cluster).
The [`kafka-with-kraft.yaml`](./kafka-with-kraft.yaml) deploys a Kafka cluster with one pool of _KRaft controller_ nodes and one pool of _KRaft broker_ nodes.
The [`kafka-with-dual-role-kraft-nodes.yaml`](./kafka-with-dual-role-kraft-nodes.yaml) deploys a Kafka cluster with one pool of KRaft nodes that share the _broker_ and _controller_ roles.

_NOTE: To use this example, you have to enable the `UseKRaft` feature gate in addition to the `KafkaNodePools` feature gate._

Please be aware that ZooKeeper-less Kafka is still a work in progress and is still missing many features.
For example:
* The controller-only nodes are currently not rolled by Strimzi when their configuration changes due to the limitations of the Kafka Admin API.
* JBOD storage is not supported (you can use the `type: jbod` storage in the Strimzi custom resources, but it should contain only a single volume)
