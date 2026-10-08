---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaConnect resource with a build listing connector plugin artifacts and an output image
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: creates
        to: Not ready
        facts:
          - Name
          - Replicas
          - Kafka Connect version
          - Bootstrap servers
          - TLS and authentication
          - Connect configuration
          - Build
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
  - text: The Product builds an image containing the plugins and pushes it to the output registry
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: reads
        facts:
          - Build
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
  - text: The Product starts the workers from the built image and reports the Kafka Connect cluster ready
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        from: Not ready
        to: Ready
        facts:
          - REST API URL
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
---

# Deploy Kafka Connect with plugins built into its image

## Trigger

The connector plugins needed are not in the standard image.

## Outcome

The workers run with the listed plugins available.

## Edge cases

- An artifact whose checksum does not match fails the build and leaves the resource Not ready.
