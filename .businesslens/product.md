---
id: strimzi
summary: Run Apache Kafka on Kubernetes and OpenShift by declaring Kafka clusters, node pools, topics, users and Kafka components as custom resources that the Strimzi operators reconcile.
category: kubernetes-operator
tags:
  - kafka
  - kubernetes
  - openshift
  - operator
  - cncf
authors:
  - name: Strimzi authors
    url: https://strimzi.io
license: Apache-2.0
limitations:
  - Kafka clients produce and consume through the Kafka brokers directly; Strimzi deploys and configures the brokers but never carries client traffic.
  - Who may create, change, delete or read Strimzi custom resources and the Secrets Strimzi creates is decided by Kubernetes RBAC, not by Strimzi; Strimzi ships the optional strimzi-admin and strimzi-view cluster roles for it.
  - Kafka clusters run in KRaft mode only.
  - Rebalancing and replication factor changes use Cruise Control, which Strimzi deploys beside a Kafka cluster only when the cluster asks for it.
  - 'Changes take effect asynchronously: the operators reconcile each custom resource and report the outcome in its status.'
  - Strimzi deploys the HTTP Bridge, Kafka Connect and MirrorMaker 2; their own runtime APIs come from their own releases.
  - Strimzi runs inside an existing Kubernetes cluster and never provisions Kubernetes nodes, storage classes or load balancers.
  - The Topic Operator and User Operator can also run standalone for a Kafka cluster that the Cluster Operator does not deploy.
references:
  - kind: doc
    role: context
    target: README.md
  - kind: doc
    role: context
    target: documentation/assemblies/deploying/assembly-deploy-intro-custom-resources.adoc
  - kind: doc
    role: context
    target: documentation/modules/deploying/proc-deploy-designating-strimzi-administrators.adoc
---

# Strimzi

Strimzi runs Apache Kafka on Kubernetes and OpenShift. People describe the Kafka clusters, node pools, topics, users, Kafka Connect clusters and connectors, MirrorMaker 2 deployments, HTTP Bridges and rebalances they want as Kubernetes custom resources; the Cluster, Topic and User Operators create and keep the matching Kafka infrastructure, report its state back in each resource's status, and carry out rolling updates, scaling, upgrades and certificate renewal safely on their behalf.

## Intent

Let a Kubernetes team operate Kafka declaratively, with the same tools and permissions they use for the rest of their platform, while the operators take care of the operational steps that are easy to get wrong by hand — rolling brokers one at a time, keeping partitions available, renewing certificates and moving data before brokers are removed.
