---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaUser resource with tls-external authentication
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
  - text: The Product applies the ACL rules and quotas for the certificate's user name without issuing credentials, and reports the user ready
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: changes
        from: Not ready
        to: Ready
        facts:
          - Username
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

# Create a user authenticated by an externally issued certificate

## Trigger

Client certificates come from another issuer, such as cert-manager.

## Outcome

The user has the declared access; no user secret is created.
