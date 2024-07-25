# TLS 
 
## TLS protocol

The `tls` label ensures proper handling of TLS (Transport Layer Security) across all components. 
This involves configuring certificate sources and validating that components correctly handle TLS communication, 
ensuring secure and encrypted data transmission.

#### Label: `tls`

This label is used for tests that ensure proper handling of TLS (Transport Layer Security) across all components. This includes configuring certificate sources and validating TLS settings to ensure secure and encrypted data transmission.

## Key Aspects Covered
1. Setup and Configuration:
   1. Deploying necessary resources and configurations.
   2. Creating and configuring TLS settings across components.
2. TLS Communication:
   1. Verifying TLS communication with different components.
   2. Configuring certificate sources and ensuring components correctly handle TLS communication.

<!-- generated part -->
**Tests:**
- [testReceiveSimpleMessageTlsScramSha](../../.././development-docs/systemtests/io.strimzi.systemtest.bridge.HttpBridgeScramShaST.md)
- [testSendSimpleMessageTls](../../.././development-docs/systemtests/io.strimzi.systemtest.bridge.HttpBridgeTlsST.md)
- [testReceiveSimpleMessageTls](../../.././development-docs/systemtests/io.strimzi.systemtest.bridge.HttpBridgeTlsST.md)
- [testSendSimpleMessageTlsScramSha](../../.././development-docs/systemtests/io.strimzi.systemtest.bridge.HttpBridgeScramShaST.md)
