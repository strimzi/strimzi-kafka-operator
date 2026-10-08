---
availability:
  - place: custom-resources
domain: bridge
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaBridgeAssemblyOperator.java#KafkaBridgeAssemblyOperator
  - kind: doc
    role: context
    target: documentation/assemblies/deploying/assembly-deploy-http-bridge.adoc
---

# Deploy an HTTP Bridge

Create an HTTP Bridge that gives HTTP clients access to a Kafka cluster.
