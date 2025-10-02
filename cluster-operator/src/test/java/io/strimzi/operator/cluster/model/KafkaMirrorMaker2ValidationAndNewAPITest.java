/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha512;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpecBuilder;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaMirrorMaker2ValidationAndNewAPITest {
    private final static KafkaMirrorMaker2 OLD_KMM2 = new KafkaMirrorMaker2Builder()
            .withNewMetadata()
                .withName("my-mm2")
                .withNamespace("my-namespace")
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withConnectCluster("target")
                .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder()
                                .withAlias("source")
                                .withBootstrapServers("source:9092")
                                .build(),
                        new KafkaMirrorMaker2ClusterSpecBuilder()
                                .withAlias("target")
                                .withBootstrapServers("target:9092")
                                .build())
                .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                        .withSourceCluster("source")
                        .withTargetCluster("target")
                        .withNewSourceConnector()
                            .withTasksMax(5)
                            .withConfig(Map.of("sync.topic.acls.enabled", "false"))
                        .endSourceConnector()
                        .withNewCheckpointConnector()
                            .withTasksMax(3)
                            .withConfig(Map.of("sync.group.offsets.enabled", "true"))
                        .endCheckpointConnector()
                        .withNewHeartbeatConnector()
                            .withTasksMax(1)
                        .endHeartbeatConnector()
                        .withTopicsPattern("my-topic-.*")
                        .withTopicsExcludePattern("exclude-topic-.*")
                        .withGroupsPattern("my-group-.*")
                        .withGroupsExcludePattern("exclude-group-.*")
                        .build())
            .endSpec()
            .build();

    private final static KafkaMirrorMaker2 KMM2 = new KafkaMirrorMaker2Builder()
            .withNewMetadata()
                .withName("my-mm2")
                .withNamespace("my-namespace")
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewTarget()
                    .withAlias("target")
                    .withGroupId("my-mm2-group")
                    .withConfigStorageTopic("my-mm2-config")
                    .withOffsetStorageTopic("my-mm2-offset")
                    .withStatusStorageTopic("my-mm2-status")
                    .withBootstrapServers("target:9092")
                .endTarget()
                .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                        .withNewSource()
                            .withAlias("source")
                            .withBootstrapServers("source:9092")
                        .endSource()
                        .withNewSourceConnector()
                            .withTasksMax(5)
                            .withConfig(Map.of("sync.topic.acls.enabled", "false"))
                        .endSourceConnector()
                        .withNewCheckpointConnector()
                            .withTasksMax(3)
                            .withConfig(Map.of("sync.group.offsets.enabled", "true"))
                        .endCheckpointConnector()
                        .withNewHeartbeatConnector()
                            .withTasksMax(1)
                        .endHeartbeatConnector()
                        .withTopicsPattern("my-topic-.*")
                        .withTopicsExcludePattern("exclude-topic-.*")
                        .withGroupsPattern("my-group-.*")
                        .withGroupsExcludePattern("exclude-group-.*")
                        .build())
            .endSpec()
            .build();

    @Test
    public void testNewLayoutValidation()    {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2).build();
        assertDoesNotThrow(() -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
    }

    @Test
    public void testSpecSectionMissing() {
        // Missing spec
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .withSpec(null)
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is(".spec section is required for KafkaMirrorMaker2 resource"));
    }

    @Test
    public void testMirrorsEmptyList() {
        // Missing mirrors
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .withMirrors(List.of())
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is(".spec.mirrors section is required in KafkaMirrorMaker2 resource"));
    }

    @Test
    public void testMirrorsNull() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .withMirrors((List<KafkaMirrorMaker2MirrorSpec>) null)
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is(".spec.mirrors section is required in KafkaMirrorMaker2 resource"));
    }

    @Test
    public void testClustersEmptyList() {
        // Missing clusters
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(OLD_KMM2)
                .editSpec()
                    .withClusters(List.of())
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is("Either .spec.target or .spec.connectCluster and .spec.clusters have to be specified"));
    }

    @Test
    public void testClustersNull() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(OLD_KMM2)
                .editSpec()
                    .withClusters((List<KafkaMirrorMaker2ClusterSpec>) null)
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is("Either .spec.target or .spec.connectCluster and .spec.clusters have to be specified"));
    }

    @Test
    public void testNullConnectCluster() {
        // Missing connect cluster
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(OLD_KMM2)
                .editSpec()
                    .withConnectCluster(null)
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is("Either .spec.target or .spec.connectCluster and .spec.clusters have to be specified"));
    }

    @Test
    public void testMissingConnectCluster() {
        // Non-existent connect cluster
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(OLD_KMM2)
                .editSpec()
                    .withConnectCluster("do-not-exist")
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is("connectCluster with alias do-not-exist cannot be found in the list of clusters at .spec.clusters"));
    }

    @Test
    public void testMissingTargetCluster() {
        // Missing alias
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(OLD_KMM2)
                .editSpec()
                    .editMirror(0)
                        .withTargetCluster("wrong-target")
                    .endMirror()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is("The .spec.mirrors[].targetCluster alias is not the same as the actual target cluster alias."));
    }

    @Test
    public void testMissingSourceCluster() {
        // Missing alias
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(OLD_KMM2)
                .editSpec()
                    .editMirror(0)
                        .withSourceCluster("wrong-target")
                    .endMirror()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is(".spec.mirrors[].sourceCluster is set to an non-existent alias. Cannot determine source cluster."));
    }

    @Test
    public void testNullSourceCluster() {
        // Missing alias
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(OLD_KMM2)
                .editSpec()
                    .editMirror(0)
                        .withSourceCluster(null)
                    .endMirror()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is("Both .spec.mirrors[].source and .spec.mirrors[].sourceCluster are missing. Cannot determine source cluster."));
    }

    @Test
    public void testSourceAsConnectCluster() {
        // The most obvious error case, where the Connect cluster is set to the source cluster instead of the target cluster
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(OLD_KMM2)
                .editSpec()
                    .withConnectCluster("source")
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is("The .spec.mirrors[].targetCluster alias is not the same as the actual target cluster alias."));
    }

    @Test
    public void testTargetAndConnectClusterDiffer() {
        // A case where one mirror has the correct target cluster, but the other does not
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(OLD_KMM2)
                .editSpec()
                .addToClusters(new KafkaMirrorMaker2ClusterSpecBuilder()
                                .withAlias("third")
                                .withBootstrapServers("third:9092")
                                .build())
                .addToMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                        .withSourceCluster("source")
                        .withTargetCluster("third").build())
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is("The .spec.mirrors[].targetCluster alias is not the same as the actual target cluster alias."));
    }

    @Test
    public void testTargetClusterNotConnectCluster() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .withConnectCluster("target")
                    .addToClusters(new KafkaMirrorMaker2ClusterSpecBuilder()
                        .withAlias("third")
                        .withBootstrapServers("source:9092")
                        .build())
                    .editMirror(0)
                        .withTargetCluster("third")
                    .endMirror()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is("The .spec.mirrors[].targetCluster alias is not the same as the actual target cluster alias."));
    }

    @Test
    public void testDuplicateAliases() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .addToMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withNewSource()
                                .withAlias("source")
                                .withBootstrapServers("source2:9092")
                            .endSource()
                            .withNewSourceConnector()
                                .withTasksMax(5)
                                .withConfig(Map.of("sync.topic.acls.enabled", "false"))
                            .endSourceConnector()
                            .build())
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is("The target and source cluster aliases must be unique."));
    }

    @Test
    public void testMultipleMirrors() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .addToMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withNewSource()
                                .withAlias("source2")
                                .withBootstrapServers("source2:9092")
                            .endSource()
                            .withNewSourceConnector()
                                .withTasksMax(5)
                                .withConfig(Map.of("sync.topic.acls.enabled", "false"))
                            .endSourceConnector()
                            .withNewCheckpointConnector()
                                .withTasksMax(3)
                                .withConfig(Map.of("sync.group.offsets.enabled", "true"))
                            .endCheckpointConnector()
                            .withNewHeartbeatConnector()
                                .withTasksMax(1)
                            .endHeartbeatConnector()
                            .withTopicsPattern("my-topic-.*")
                            .withTopicsExcludePattern("exclude-topic-.*")
                            .withGroupsPattern("my-group-.*")
                            .withGroupsExcludePattern("exclude-group-.*")
                            .build())
                .endSpec()
                .build();

        assertDoesNotThrow(() -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
    }

    @Test
    public void testConflictingMirrors() {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder(KMM2)
                .editSpec()
                    .addToMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withNewSource()
                                .withAlias("source")
                                .withBootstrapServers("source:9092")
                            .endSource()
                            .withNewSourceConnector()
                                .withTasksMax(5)
                                .withConfig(Map.of("sync.topic.acls.enabled", "false"))
                            .endSourceConnector()
                            .withNewCheckpointConnector()
                                .withTasksMax(3)
                                .withConfig(Map.of("sync.group.offsets.enabled", "true"))
                            .endCheckpointConnector()
                            .withNewHeartbeatConnector()
                                .withTasksMax(1)
                            .endHeartbeatConnector()
                            .withTopicsPattern("my-topic-.*")
                            .withTopicsExcludePattern("exclude-topic-.*")
                            .withGroupsPattern("my-group-.*")
                            .withGroupsExcludePattern("exclude-group-.*")
                            .build())
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2));
        assertThat(ex.getMessage(), is("The target and source cluster aliases must be unique."));
    }

    @SuppressWarnings("deprecation") // Clusters, connectCluster, sourceCluster and targetCluster are deprecated
    @Test
    public void testConversion()    {
        KafkaMirrorMaker2 kmm2 = new KafkaMirrorMaker2Builder()
            .withNewMetadata()
                .withName("my-mm2")
                .withNamespace("my-namespace")
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withConnectCluster("target")
                .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder()
                                .withAlias("source")
                                .withBootstrapServers("source:9093")
                                .withNewTls()
                                    .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("source-ca").withCertificate("ca.crt").build())
                                .endTls()
                                .withNewKafkaClientAuthenticationScramSha512()
                                    .withUsername("source-user")
                                    .withNewPasswordSecret()
                                        .withSecretName("source-user-secret")
                                        .withPassword("passwordKey")
                                    .endPasswordSecret()
                                .endKafkaClientAuthenticationScramSha512()
                                .withConfig(Map.of("refresh.groups.interval.seconds", "600"))
                                .build(),
                        new KafkaMirrorMaker2ClusterSpecBuilder()
                                .withAlias("target")
                                .withBootstrapServers("target:9093")
                                .withNewTls()
                                    .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("target-ca").withCertificate("ca.crt").build())
                                .endTls()
                                .withNewKafkaClientAuthenticationScramSha512()
                                    .withUsername("target-user")
                                    .withNewPasswordSecret()
                                        .withSecretName("target-user-secret")
                                        .withPassword("passwordKey")
                                    .endPasswordSecret()
                                .endKafkaClientAuthenticationScramSha512()
                                .withConfig(Map.of("config.storage.replication.factor", "-1", "offset.storage.replication.factor", "-1", "status.storage.replication.factor", "-1"))
                                .build())
                .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                        .withSourceCluster("source")
                        .withTargetCluster("target")
                        .withNewSourceConnector()
                            .withTasksMax(5)
                            .withConfig(Map.of("sync.topic.acls.enabled", "false"))
                        .endSourceConnector()
                        .withNewCheckpointConnector()
                            .withTasksMax(3)
                            .withConfig(Map.of("sync.group.offsets.enabled", "true"))
                        .endCheckpointConnector()
                        .withTopicsPattern("my-topic-.*")
                        .withGroupsPattern("my-group-.*")
                        .build())
            .endSpec()
            .build();

        assertDoesNotThrow(() -> {
            KafkaMirrorMaker2 updated = KafkaMirrorMaker2Cluster.validateAndUpdateToNewAPI(kmm2);

            // Removed values
            assertThat(updated.getSpec().getReplicas(), is(3));
            assertThat(updated.getSpec().getConnectCluster(), is(nullValue()));
            assertThat(updated.getSpec().getClusters(), is(nullValue()));
            assertThat(updated.getSpec().getMirrors().get(0).getSourceCluster(), is(nullValue()));
            assertThat(updated.getSpec().getMirrors().get(0).getTargetCluster(), is(nullValue()));

            // New values
            assertThat(updated.getSpec().getTarget(), is(notNullValue()));
            assertThat(updated.getSpec().getTarget().getAlias(), is("target"));
            assertThat(updated.getSpec().getTarget().getBootstrapServers(), is("target:9093"));
            assertThat(updated.getSpec().getTarget().getTls().getTrustedCertificates().get(0).getSecretName(), is("target-ca"));
            assertThat(updated.getSpec().getTarget().getTls().getTrustedCertificates().get(0).getCertificate(), is("ca.crt"));
            assertThat(updated.getSpec().getTarget().getAuthentication().getType(), is(KafkaClientAuthenticationScramSha512.TYPE_SCRAM_SHA_512));
            assertThat(((KafkaClientAuthenticationScramSha512) updated.getSpec().getTarget().getAuthentication()).getUsername(), is("target-user"));
            assertThat(((KafkaClientAuthenticationScramSha512) updated.getSpec().getTarget().getAuthentication()).getPasswordSecret().getSecretName(), is("target-user-secret"));
            assertThat(((KafkaClientAuthenticationScramSha512) updated.getSpec().getTarget().getAuthentication()).getPasswordSecret().getPassword(), is("passwordKey"));
            assertThat(updated.getSpec().getTarget().getConfig(), is(Map.of("config.storage.replication.factor", "-1", "offset.storage.replication.factor", "-1", "status.storage.replication.factor", "-1")));

            assertThat(updated.getSpec().getMirrors().get(0).getSource(), is(notNullValue()));
            assertThat(updated.getSpec().getMirrors().get(0).getSource().getAlias(), is("source"));
            assertThat(updated.getSpec().getMirrors().get(0).getSource().getBootstrapServers(), is("source:9093"));
            assertThat(updated.getSpec().getMirrors().get(0).getSource().getTls().getTrustedCertificates().get(0).getSecretName(), is("source-ca"));
            assertThat(updated.getSpec().getMirrors().get(0).getSource().getTls().getTrustedCertificates().get(0).getCertificate(), is("ca.crt"));
            assertThat(updated.getSpec().getMirrors().get(0).getSource().getAuthentication().getType(), is(KafkaClientAuthenticationScramSha512.TYPE_SCRAM_SHA_512));
            assertThat(((KafkaClientAuthenticationScramSha512) updated.getSpec().getMirrors().get(0).getSource().getAuthentication()).getUsername(), is("source-user"));
            assertThat(((KafkaClientAuthenticationScramSha512) updated.getSpec().getMirrors().get(0).getSource().getAuthentication()).getPasswordSecret().getSecretName(), is("source-user-secret"));
            assertThat(((KafkaClientAuthenticationScramSha512) updated.getSpec().getMirrors().get(0).getSource().getAuthentication()).getPasswordSecret().getPassword(), is("passwordKey"));
            assertThat(updated.getSpec().getMirrors().get(0).getSource().getConfig(), is(Map.of("refresh.groups.interval.seconds", "600")));
        });
    }
}
