# **User Operator**

## Description

These tests cover management of KafkaUser resources by the User Operator. 
They verify user authentication mechanisms (TLS, SCRAM-SHA-512, external TLS), authorization with ACLs, quota enforcement, secret management with custom prefixes, and user lifecycle operations to ensure reliable user management within a Kafka cluster.

<!-- generated part -->
**Tests:**
- [testCreatingUsersWithSecretPrefix](../io.strimzi.systemtest.operators.user.UserST.md)
- [testScalability](../io.strimzi.systemtest.performance.UserOperatorScalabilityPerformance.md)
- [testScramUserWithQuotas](../io.strimzi.systemtest.operators.user.UserST.md)
- [testTlsExternalUser](../io.strimzi.systemtest.operators.user.UserST.md)
- [testTlsExternalUserWithQuotas](../io.strimzi.systemtest.operators.user.UserST.md)
- [testTlsUserWithQuotas](../io.strimzi.systemtest.operators.user.UserST.md)
- [testUpdateUser](../io.strimzi.systemtest.operators.user.UserST.md)
- [testUserWithNameMoreThan64Chars](../io.strimzi.systemtest.operators.user.UserST.md)
- [testUserWithQuotas](../io.strimzi.systemtest.operators.user.UserST.md)
