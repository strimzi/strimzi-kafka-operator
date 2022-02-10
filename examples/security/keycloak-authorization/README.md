# Keycloak authorization example

This folder contains an example `Kafka` custom resource configured for OAuth 2.0 token-based authorization using Keycloak. The example resource is configured with: 
- `keycloak` authorization 
- the corresponding `oauth` authentication
The folder also contains a Keycloak realm export to import into your Keycloak instance to support the example.

Full instructions for the example are available in the [Strimzi Documentation](https://strimzi.io/docs/operators/0.28.0/using.html#con-oauth-authorization-keycloak-example).

* [kafka-authz-realm.json](./kafka-authz-realm.json)
    * The Keycloak realm export file
* [kafka-ephemeral-oauth-single-keycloak-authz.yaml](./kafka-ephemeral-oauth-single-keycloak-authz.yaml)
    * The Kafka CR that defines a single-node Kafka cluster with `oauth` authentication and `keycloak` authorization,
    using the `kafka-authz` realm. See [full example instructions](https://strimzi.io/docs/operators/0.28.0/using.html#con-oauth-authorization-keycloak-example) for proper preparation and deployment.
