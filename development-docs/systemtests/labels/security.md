# Security

## Description

These tests validate security-related functionality in the Strimzi ecosystem.
They cover authentication and authorization mechanisms (OAuth, ACLs, OPA integration), TLS configuration, custom Certificate Authority (CA) management, network policies, and pod security profiles.

<!-- generated part -->
**Tests:**
- [smokeTestForClients](../io.strimzi.systemtest.security.oauth.OauthAuthorizationST.md)
- [testAclRuleReadAndWrite](../io.strimzi.systemtest.security.custom.CustomAuthorizerST.md)
- [testAclWithSuperUser](../io.strimzi.systemtest.security.custom.CustomAuthorizerST.md)
- [testCustomCaTrustChainOnInternalPort](../io.strimzi.systemtest.security.custom.CustomCaChainST.md)
- [testCustomClusterCaAndClientsCaCertificates](../io.strimzi.systemtest.security.custom.CustomCaST.md)
- [testKafkaConnectTrustWithCustomCaChain](../io.strimzi.systemtest.security.custom.CustomCaChainST.md)
- [testMultistageCustomCaTrustChainEstablishment](../io.strimzi.systemtest.security.custom.CustomCaChainST.md)
- [testMultistageCustomCaUserCertificateAuthentication](../io.strimzi.systemtest.security.custom.CustomCaChainST.md)
- [testProducerConsumerBridgeWithOauthMetrics](../io.strimzi.systemtest.security.oauth.OauthPlainST.md)
- [testProducerConsumerConnectWithOauthMetrics](../io.strimzi.systemtest.security.oauth.OauthPlainST.md)
- [testProducerConsumerMirrorMaker2WithOauthMetrics](../io.strimzi.systemtest.security.oauth.OauthPlainST.md)
- [testProducerConsumerWithOauthMetrics](../io.strimzi.systemtest.security.oauth.OauthPlainST.md)
- [testReplaceCustomClientsCACertificateValidityToInvokeRenewalProcess](../io.strimzi.systemtest.security.custom.CustomCaST.md)
- [testReplaceCustomClusterCACertificateValidityToInvokeRenewalProcess](../io.strimzi.systemtest.security.custom.CustomCaST.md)
- [testReplacingCustomClientsKeyPairToInvokeRenewalProcess](../io.strimzi.systemtest.security.custom.CustomCaST.md)
- [testReplacingCustomClusterKeyPairToInvokeRenewalProcess](../io.strimzi.systemtest.security.custom.CustomCaST.md)
- [testSaslPlainProducerConsumer](../io.strimzi.systemtest.security.oauth.OauthPlainST.md)
- [testTeamAReadFromTopic](../io.strimzi.systemtest.security.oauth.OauthAuthorizationST.md)
- [testTeamAWriteToTopic](../io.strimzi.systemtest.security.oauth.OauthAuthorizationST.md)
- [testTeamAWriteToTopicStartingWithXAndTeamBReadFromTopicStartingWithX](../io.strimzi.systemtest.security.oauth.OauthAuthorizationST.md)
- [testTeamBWriteToTopic](../io.strimzi.systemtest.security.oauth.OauthAuthorizationST.md)
