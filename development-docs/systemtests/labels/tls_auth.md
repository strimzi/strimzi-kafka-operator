# TLS Authentication

## tls_auth

The `tls_auth` label ensures proper TLS authentication across all components, not limited to Kafka Bridge. This involves configuring certificate sources and validating that components correctly handle TLS authentication.

#### Label: `tls_auth`

This label is used for tests that ensure proper TLS authentication across all components. This includes configuring certificate sources and validating TLS settings to ensure secure communication.

## Key Aspects Covered
1. Setup and Configuration:
2. Deploying necessary resources and configurations.
3. Creating and configuring TLS settings across components.
4. TLS Authentication:
   1. Verifying TLS authentication with different components.
   2. Configuring certificate sources and ensuring components correctly handle TLS authentication.


<!-- generated part -->
**Tests:**
- [testTlsAuthWithWeirdUsername](../../.././development-docs/systemtests/io.strimzi.systemtest.bridge.HttpBridgeKafkaExternalListenersST.md)
