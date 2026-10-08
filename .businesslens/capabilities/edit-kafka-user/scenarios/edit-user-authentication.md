---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the authentication in the KafkaUser resource from TLS to SCRAM-SHA-512
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: changes
        facts:
          - Authentication
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The Product generates a user password, registers it in Kafka and replaces the user certificate in the user secret with it
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: user-certificate
        effect: removes
      - entity: user-password
        effect: creates
        facts:
          - Password
          - JAAS configuration
    contexts:
      api:
        place: custom-resources::user-secret
  - text: The Product moves the user's ACLs and quotas to the new username and reports it
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: changes
        facts:
          - Username
    contexts:
      api:
        place: custom-resources::kafka-user-resource
---

# Change a user from TLS to SCRAM-SHA-512 authentication

## Trigger

An application switches from certificates to a password.

## Outcome

The user secret holds the user password and the old certificate no longer authenticates the user.

## Edge cases

- Changing to tls-external authentication deletes the user secret.
