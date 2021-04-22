# Keycloak authorization example

This folder contains an example of Strimzi custom resource with configured `keycloak` authorization and a corresponding `oauth` authentication,
and a Keycloak realm export that should be imported into your Keycloak instance to support the example.

Full example instructions are available in [Strimzi Documentation](https://strimzi.io/docs/operators/in-development/using.html#con-oauth-authorization-keycloak-example).

* [kafka-authz-realm.json](./kafka-authz-realm.json)
    * The Keycloak realm export file
* [kafka-ephemeral-oauth-single-keycloak-authz.yaml](./kafka-ephemeral-oauth-single-keycloak-authz.yaml)
    * The Kafka CR that defines a single-node Kafka cluster with `oauth` authentication and `keycloak` authorization,
    using the `kafka-authz` realm. See [full example instructions](https://strimzi.io/docs/operators/in-development/using.html#con-oauth-authorization-keycloak-example) for proper preparation and deployment.