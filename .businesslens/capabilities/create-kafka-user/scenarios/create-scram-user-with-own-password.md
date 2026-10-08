---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaUser resource with SCRAM-SHA-512 authentication and a Secret supplying the password
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: creates
        to: Not ready
        facts:
          - Name
          - Authentication
          - Password source
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
  - text: The Product registers the supplied password in Kafka and stores the user password in the user secret
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

# Create a user with a supplied SCRAM-SHA-512 password

## Trigger

The password is managed outside Strimzi.

## Outcome

The application can connect with the supplied password.

## Edge cases

- A missing password Secret, a missing key or an empty password leaves the user Not ready.
- A later change to the supplying Secret is picked up at the next periodic reconciliation.
