# Plaintext

The `plaintext` label focuses on testing communication mechanisms over unencrypted PLAINTEXT channels. 
This includes ensuring that components handle PLAINTEXT communication correctly and securely, 
despite the lack of encryption.

#### Label: `plaintext`

This label is used for tests that verify the correct handling of communication mechanisms over PLAINTEXT channels. 
The tests ensure that the system processes data transmission properly and securely, even without the encryption provided by TLS.

## Key Aspects Covered
1. Setup and Configuration:
   1. Deploying necessary resources and configurations.
   2. Creating and configuring settings for PLAINTEXT communication.
2. PLAINTEXT Communication:
   1. Verifying that components correctly handle data transmission over PLAINTEXT.
   2. Ensuring secure handling of data, despite the lack of encryption.



<!-- generated part -->
**Tests:**
- [testSendSimpleMessage](../../.././development-docs/systemtests/io.strimzi.systemtest.bridge.HttpBridgeST.md)
