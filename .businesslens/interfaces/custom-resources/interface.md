---
type: api
actors:
  - strimzi-administrator
  - drain-cleaner
references:
  - kind: doc
    role: context
    target: documentation/modules/managing/con-custom-resources-status.adoc
  - kind: doc
    role: context
    target: packaging/install/cluster-operator/040-Crd-kafka.yaml
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/AbstractOperator.java#AbstractOperator
---

# Strimzi custom resources

The Kubernetes API extended with Strimzi's custom resource definitions. People and tools create, change and delete `Kafka`, `KafkaNodePool`, `KafkaTopic`, `KafkaUser`, `KafkaConnect`, `KafkaConnector`, `KafkaMirrorMaker2`, `KafkaBridge` and `KafkaRebalance` resources, set `strimzi.io/` annotations on them and on the pods, StrimziPodSets and Secrets Strimzi creates, and read each resource's status to see the outcome.

## Intent

Give Kafka a declarative contract that works with any Kubernetes client — kubectl, GitOps tools, Helm — and with the access control Kubernetes already applies.
