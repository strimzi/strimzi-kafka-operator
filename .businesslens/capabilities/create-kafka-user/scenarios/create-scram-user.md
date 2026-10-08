---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaUser resource with SCRAM-SHA-512 authentication
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: creates
        to: Not ready
        facts:
          - Name
          - Authentication
          - Quotas
      - entity: acl-rule
        effect: creates
        facts:
          - Resource
          - Operations
          - Host
          - Type
        with: kafka-user
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The Product generates a password, registers the SCRAM credentials in Kafka and stores the user password in the user secret
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: user-password
        effect: creates
        facts:
          - Password
          - JAAS configuration
    contexts:
      api:
        place: custom-resources::user-secret
  - text: The Product applies the ACL rules and quotas and reports the user ready
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: changes
        from: Not ready
        to: Ready
        facts:
          - Username
          - Credentials Secret
      - entity: acl-rule
        effect: reads
        facts:
          - Resource
          - Operations
          - Host
          - Type
    contexts:
      api:
        place: custom-resources::kafka-user-resource
---

# Create a user with a generated SCRAM-SHA-512 password

## Trigger

A client application authenticates with a username and password.

## Outcome

The application can connect with the user password from the user secret.
