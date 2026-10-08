---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator deletes the Kafka resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: removes
        from: Ready
      - entity: listener
        effect: removes
        with: kafka-cluster
      - entity: certificate-authority
        as: cluster-ca
        effect: removes
        with: kafka-cluster
      - entity: certificate-authority
        as: clients-ca
        effect: removes
        with: kafka-cluster
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product's pods, services and components for the cluster are removed
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: removes
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Delete a Kafka cluster

## Trigger

The Strimzi administrator no longer needs the Kafka cluster.

## Outcome

The cluster no longer runs; its volume claims stay until their node pools are deleted.

## Edge cases

- KafkaNodePool, KafkaTopic and KafkaUser resources are separate resources and remain until they are deleted.
- CA Secrets of a certificate authority whose Secret owner reference is turned off remain, so that a new cluster can reuse the CA.
