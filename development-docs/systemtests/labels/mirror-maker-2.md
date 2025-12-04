# **Mirror Maker 2**

## Description

These tests validate Kafka MirrorMaker 2 cross-cluster replication. 
They cover secure connectivity (TLS+mTLS, TLS+SCRAM), message/header mirroring, scaling (including scale-to-zero), 
topic partition propagation, identity replication policy, offset checkpoint/restore in active-active setups, 
connector state transitions (i.e., failed <-> running, pause or resume), manual and secret/cert-driven rolling updates, 
and error handling via status or conditions and offset introspection.

<!-- generated part -->
**Tests:**
- [testIdentityReplicationPolicy](../io.strimzi.systemtest.mirrormaker.MirrorMaker2ST.md)
- [testKMM2RollAfterSecretsCertsUpdateScramSha](../io.strimzi.systemtest.mirrormaker.MirrorMaker2ST.md)
- [testKMM2RollAfterSecretsCertsUpdateTLS](../io.strimzi.systemtest.mirrormaker.MirrorMaker2ST.md)
- [testKafkaMirrorMaker2ConnectorsStateAndOffsetManagement](../io.strimzi.systemtest.mirrormaker.MirrorMaker2ST.md)
- [testKafkaMirrorMaker2Status](../io.strimzi.systemtest.operators.CustomResourceStatusST.md)
- [testKafkaMirrorMaker2WrongBootstrap](../io.strimzi.systemtest.operators.CustomResourceStatusST.md)
- [testLeaderElection](../io.strimzi.systemtest.operators.LeaderElectionST.md)
- [testMirrorMaker2](../io.strimzi.systemtest.mirrormaker.MirrorMaker2ST.md)
- [testMirrorMaker2LogSetting](../io.strimzi.systemtest.log.LogSettingST.md)
- [testMirrorMaker2Metrics](../io.strimzi.systemtest.metrics.MetricsST.md)
- [testMirrorMaker2TlsAndScramSha512Auth](../io.strimzi.systemtest.mirrormaker.MirrorMaker2ST.md)
- [testMirrorMaker2TlsAndTlsClientAuth](../io.strimzi.systemtest.mirrormaker.MirrorMaker2ST.md)
- [testRestoreOffsetsInConsumerGroup](../io.strimzi.systemtest.mirrormaker.MirrorMaker2ST.md)
- [testScaleMirrorMaker2UpAndDownToZero](../io.strimzi.systemtest.mirrormaker.MirrorMaker2ST.md)
