# Security

## Description

These tests validate security-related functionality in the Strimzi ecosystem.
They cover authentication and authorization mechanisms (OAuth, ACLs, OPA integration), TLS configuration, custom Certificate Authority (CA) management, network policies, and pod security profiles.

<!-- generated part -->
**Tests:**
- [testAclRuleReadAndWrite](../io.strimzi.systemtest.security.custom.CustomAuthorizerST.md)
- [testAclWithSuperUser](../io.strimzi.systemtest.security.custom.CustomAuthorizerST.md)
- [testCustomClusterCaAndClientsCaCertificates](../io.strimzi.systemtest.security.custom.CustomCaST.md)
- [testReplaceCustomClientsCACertificateValidityToInvokeRenewalProcess](../io.strimzi.systemtest.security.custom.CustomCaST.md)
- [testReplaceCustomClusterCACertificateValidityToInvokeRenewalProcess](../io.strimzi.systemtest.security.custom.CustomCaST.md)
- [testReplacingCustomClientsKeyPairToInvokeRenewalProcess](../io.strimzi.systemtest.security.custom.CustomCaST.md)
- [testReplacingCustomClusterKeyPairToInvokeRenewalProcess](../io.strimzi.systemtest.security.custom.CustomCaST.md)
