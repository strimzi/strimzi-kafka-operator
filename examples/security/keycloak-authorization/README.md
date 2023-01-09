# Keycloak authorization example

This folder contains an example `Kafka` custom resource configured for OAuth 2.0 token-based authorization using Keycloak. The example resource is configured with:

- `keycloak` authorization
- the corresponding `oauth` authentication

The folder also contains a Keycloak realm export to import into your Keycloak instance to support the example.

Full instructions for the example are available in the [Strimzi Documentation](https://strimzi.io/docs/operators/0.32.0/configuring.html#proc-oauth-authorization-keycloak-example_str).

- [kafka-authz-realm.json](./kafka-authz-realm.json)
  - The Keycloak realm export file
- [kafka-ephemeral-oauth-single-keycloak-authz.yaml](./kafka-ephemeral-oauth-single-keycloak-authz.yaml)
  - The Kafka CR that defines a single-node Kafka cluster with `oauth` authentication and `keycloak` authorization,
    using the `kafka-authz` realm. See [full example instructions](https://strimzi.io/docs/operators/0.32.0/configuring.html#proc-oauth-authorization-keycloak-example_str) for proper preparation and deployment.

More examples are available at the oauth module [example documentation](https://github.com/strimzi/strimzi-kafka-oauth/tree/main/examples).
