---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaUser resource labelled with its cluster, with TLS authentication, ACL rules and quotas
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
  - text: The Product issues a user certificate signed by the clients CA and stores it in the user secret
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: certificate-authority
        effect: reads
        facts:
          - CA certificate
          - Validity days
      - entity: user-certificate
        effect: creates
        facts:
          - Certificate
          - Private key
          - 'PKCS #12 store'
          - Clients CA certificate
          - Expiry
    contexts:
      api:
        place: custom-resources::user-secret
  - text: The Product applies the ACL rules and quotas in Kafka and reports the user ready with its username and user secret
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

# Create a user with mutual TLS authentication

## Trigger

A client application needs its own identity in Kafka.

## Outcome

The application can connect with the certificate from the user secret and has exactly the declared access.

## Edge cases

- A KafkaUser resource with TLS authentication and a name longer than 64 characters is refused and stays Not ready.
