/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import io.strimzi.api.kafka.model.common.ConnectorState;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpecBuilder;
import io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("deprecation")
class MirrorMaker2ConversionsTest extends AbstractConversionsTest {
    @Test
    public void testEnforceOAuthInClusters() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
                .withNewSpec()
                    .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder().withBootstrapServers("localhost:9092").withNewKafkaClientAuthenticationOAuth().endKafkaClientAuthenticationOAuth().build())
                .endSpec()
                .build();

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.enforceOauth();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(mm2));
        assertThat(ex.getMessage(), containsString("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceOAuthInClustersJson() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
                .withNewSpec()
                    .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder().withBootstrapServers("localhost:9092").withNewKafkaClientAuthenticationOAuth().endKafkaClientAuthenticationOAuth().build())
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(mm2);

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.enforceOauth();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(json));
        assertThat(ex.getMessage(), containsString("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceOAuthInTarget() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
                .withNewSpec()
                    .withNewTarget()
                        .withBootstrapServers("localhost:9092")
                        .withNewKafkaClientAuthenticationOAuth()
                        .endKafkaClientAuthenticationOAuth()
                    .endTarget()
                .endSpec()
                .build();

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.enforceOauth();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(mm2));
        assertThat(ex.getMessage(), containsString("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceOAuthInTargetJson() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
                .withNewSpec()
                    .withNewTarget()
                        .withBootstrapServers("localhost:9092")
                        .withNewKafkaClientAuthenticationOAuth()
                        .endKafkaClientAuthenticationOAuth()
                    .endTarget()
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(mm2);

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.enforceOauth();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(json));
        assertThat(ex.getMessage(), containsString("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceOAuthInSource() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
                .withNewSpec()
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withNewSource()
                                .withBootstrapServers("localhost:9092")
                                .withNewKafkaClientAuthenticationOAuth()
                                .endKafkaClientAuthenticationOAuth()
                            .endSource()
                            .build())
                .endSpec()
                .build();

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.enforceOauth();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(mm2));
        assertThat(ex.getMessage(), containsString("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceOAuthInSourceJson() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
                .withNewSpec()
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withNewSource()
                                .withBootstrapServers("localhost:9092")
                                .withNewKafkaClientAuthenticationOAuth()
                                .endKafkaClientAuthenticationOAuth()
                            .endSource()
                            .build())
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(mm2);

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.enforceOauth();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(json));
        assertThat(ex.getMessage(), containsString("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testExcludedFields() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
                .withNewSpec()
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withGroupsBlacklistPattern("groups")
                            .withTopicsBlacklistPattern("topics")
                            .build())
                .endSpec()
                .build();

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.excludedFields();
        c.convert(mm2);

        assertThat(mm2.getSpec().getMirrors().get(0).getGroupsBlacklistPattern(), is(nullValue()));
        assertThat(mm2.getSpec().getMirrors().get(0).getGroupsExcludePattern(), is("groups"));
        assertThat(mm2.getSpec().getMirrors().get(0).getTopicsBlacklistPattern(), is(nullValue()));
        assertThat(mm2.getSpec().getMirrors().get(0).getTopicsExcludePattern(), is("topics"));
    }

    @Test
    public void testExcludedFieldsJson() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
                .withNewSpec()
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withGroupsBlacklistPattern("groups")
                            .withTopicsBlacklistPattern("topics")
                            .build())
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(mm2);

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.excludedFields();
        c.convert(json);

        KafkaMirrorMaker2 converted = jsonNodeToTyped(json, KafkaMirrorMaker2.class);
        assertThat(converted.getSpec().getMirrors().get(0).getGroupsBlacklistPattern(), is(nullValue()));
        assertThat(converted.getSpec().getMirrors().get(0).getGroupsExcludePattern(), is("groups"));
        assertThat(converted.getSpec().getMirrors().get(0).getTopicsBlacklistPattern(), is(nullValue()));
        assertThat(converted.getSpec().getMirrors().get(0).getTopicsExcludePattern(), is("topics"));
    }

    @Test
    public void testHeartbeatRemoval() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
                .withNewSpec()
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withNewSourceConnector()
                            .endSourceConnector()
                            .withNewHeartbeatConnector()
                                .withConfig(Map.of("something", "here"))
                            .endHeartbeatConnector()
                            .build())
                .endSpec()
                .build();

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.removeHeartbeatConnector();
        c.convert(mm2);

        assertThat(mm2.getSpec().getMirrors().get(0).getHeartbeatConnector(), is(nullValue()));
    }

    @Test
    public void testHeartbeatRemovalJson() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
                .withNewSpec()
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withNewSourceConnector()
                            .endSourceConnector()
                            .withNewHeartbeatConnector()
                                .withConfig(Map.of("something", "here"))
                            .endHeartbeatConnector()
                            .build())
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(mm2);

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.removeHeartbeatConnector();
        c.convert(json);

        KafkaMirrorMaker2 converted = jsonNodeToTyped(json, KafkaMirrorMaker2.class);
        assertThat(converted.getSpec().getMirrors().get(0).getHeartbeatConnector(), is(nullValue()));
    }

    @Test
    public void testPauseToState() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
                .withNewSpec()
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withNewSourceConnector()
                                .withPause(true)
                            .endSourceConnector()
                            .build())
                .endSpec()
                .build();

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.pauseToState();
        c.convert(mm2);

        assertThat(mm2.getSpec().getMirrors().get(0).getSourceConnector().getPause(), is(nullValue()));
        assertThat(mm2.getSpec().getMirrors().get(0).getSourceConnector().getState(), is(ConnectorState.PAUSED));
    }

    @Test
    public void testPauseToStateJson() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
                .withNewSpec()
                    .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                            .withNewSourceConnector()
                                .withPause(true)
                            .endSourceConnector()
                            .build())
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(mm2);

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.pauseToState();
        c.convert(json);

        KafkaMirrorMaker2 converted = jsonNodeToTyped(json, KafkaMirrorMaker2.class);
        assertThat(converted.getSpec().getMirrors().get(0).getSourceConnector().getPause(), is(nullValue()));
        assertThat(converted.getSpec().getMirrors().get(0).getSourceConnector().getState(), is(ConnectorState.PAUSED));
    }

    @Test
    public void testRestructuring() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
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
                        .build())
            .endSpec()
            .build();

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.mm2SpecRestructuring();
        c.convert(mm2);

        assertThat(mm2.getSpec().getClusters(), is(nullValue()));
        assertThat(mm2.getSpec().getConnectCluster(), is(nullValue()));
        assertThat(mm2.getSpec().getTarget().getAlias(), is("target"));
        assertThat(mm2.getSpec().getTarget().getBootstrapServers(), is("target:9092"));
        assertThat(mm2.getSpec().getTarget().getGroupId(), is("mirrormaker2-cluster"));
        assertThat(mm2.getSpec().getTarget().getConfigStorageTopic(), is("mirrormaker2-cluster-configs"));
        assertThat(mm2.getSpec().getTarget().getOffsetStorageTopic(), is("mirrormaker2-cluster-offsets"));
        assertThat(mm2.getSpec().getTarget().getStatusStorageTopic(), is("mirrormaker2-cluster-status"));
        assertThat(mm2.getSpec().getMirrors().get(0).getSourceCluster(), is(nullValue()));
        assertThat(mm2.getSpec().getMirrors().get(0).getTargetCluster(), is(nullValue()));
        assertThat(mm2.getSpec().getMirrors().get(0).getSource().getAlias(), is("source"));
        assertThat(mm2.getSpec().getMirrors().get(0).getSource().getBootstrapServers(), is("source:9092"));
    }

    @Test
    public void testRestructuringJson() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
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
                        .build())
            .endSpec()
            .build();
        JsonNode json = typedToJsonNode(mm2);

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.mm2SpecRestructuring();
        c.convert(json);

        KafkaMirrorMaker2 converted = jsonNodeToTyped(json, KafkaMirrorMaker2.class);
        assertThat(converted.getSpec().getClusters(), is(nullValue()));
        assertThat(converted.getSpec().getConnectCluster(), is(nullValue()));
        assertThat(converted.getSpec().getTarget().getAlias(), is("target"));
        assertThat(converted.getSpec().getTarget().getBootstrapServers(), is("target:9092"));
        assertThat(converted.getSpec().getTarget().getGroupId(), is("mirrormaker2-cluster"));
        assertThat(converted.getSpec().getTarget().getConfigStorageTopic(), is("mirrormaker2-cluster-configs"));
        assertThat(converted.getSpec().getTarget().getOffsetStorageTopic(), is("mirrormaker2-cluster-offsets"));
        assertThat(converted.getSpec().getTarget().getStatusStorageTopic(), is("mirrormaker2-cluster-status"));
        assertThat(converted.getSpec().getMirrors().get(0).getSourceCluster(), is(nullValue()));
        assertThat(converted.getSpec().getMirrors().get(0).getTargetCluster(), is(nullValue()));
        assertThat(converted.getSpec().getMirrors().get(0).getSource().getAlias(), is("source"));
        assertThat(converted.getSpec().getMirrors().get(0).getSource().getBootstrapServers(), is("source:9092"));
    }

    @Test
    public void testRestructuringWithSettings() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
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
                                .withConfig(Map.of(
                                        "group.id", "my-mm2-group",
                                        "config.storage.topic", "my-mm2-config",
                                        "offset.storage.topic", "my-mm2-offset",
                                        "status.storage.topic", "my-mm2-status"
                                ))
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
                        .build())
            .endSpec()
            .build();

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.mm2SpecRestructuring();
        c.convert(mm2);

        assertThat(mm2.getSpec().getClusters(), is(nullValue()));
        assertThat(mm2.getSpec().getConnectCluster(), is(nullValue()));
        assertThat(mm2.getSpec().getTarget().getAlias(), is("target"));
        assertThat(mm2.getSpec().getTarget().getBootstrapServers(), is("target:9092"));
        assertThat(mm2.getSpec().getTarget().getGroupId(), is("my-mm2-group"));
        assertThat(mm2.getSpec().getTarget().getConfigStorageTopic(), is("my-mm2-config"));
        assertThat(mm2.getSpec().getTarget().getOffsetStorageTopic(), is("my-mm2-offset"));
        assertThat(mm2.getSpec().getTarget().getStatusStorageTopic(), is("my-mm2-status"));
        assertThat(mm2.getSpec().getTarget().getConfig().get("group.id"), is(nullValue()));
        assertThat(mm2.getSpec().getTarget().getConfig().get("config.storage.topic"), is(nullValue()));
        assertThat(mm2.getSpec().getTarget().getConfig().get("offset.storage.topic"), is(nullValue()));
        assertThat(mm2.getSpec().getTarget().getConfig().get("status.storage.topic"), is(nullValue()));
        assertThat(mm2.getSpec().getMirrors().get(0).getSourceCluster(), is(nullValue()));
        assertThat(mm2.getSpec().getMirrors().get(0).getTargetCluster(), is(nullValue()));
        assertThat(mm2.getSpec().getMirrors().get(0).getSource().getAlias(), is("source"));
        assertThat(mm2.getSpec().getMirrors().get(0).getSource().getBootstrapServers(), is("source:9092"));
    }

    @Test
    public void testRestructuringWithSettingsJson() {
        KafkaMirrorMaker2 mm2 = new KafkaMirrorMaker2Builder()
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
                                .withConfig(Map.of(
                                        "group.id", "my-mm2-group",
                                        "config.storage.topic", "my-mm2-config",
                                        "offset.storage.topic", "my-mm2-offset",
                                        "status.storage.topic", "my-mm2-status"
                                ))
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
                        .build())
            .endSpec()
            .build();
        JsonNode json = typedToJsonNode(mm2);

        Conversion<KafkaMirrorMaker2> c = MirrorMaker2Conversions.mm2SpecRestructuring();
        c.convert(json);

        KafkaMirrorMaker2 converted = jsonNodeToTyped(json, KafkaMirrorMaker2.class);
        assertThat(converted.getSpec().getClusters(), is(nullValue()));
        assertThat(converted.getSpec().getConnectCluster(), is(nullValue()));
        assertThat(converted.getSpec().getTarget().getAlias(), is("target"));
        assertThat(converted.getSpec().getTarget().getBootstrapServers(), is("target:9092"));
        assertThat(converted.getSpec().getTarget().getGroupId(), is("my-mm2-group"));
        assertThat(converted.getSpec().getTarget().getConfigStorageTopic(), is("my-mm2-config"));
        assertThat(converted.getSpec().getTarget().getOffsetStorageTopic(), is("my-mm2-offset"));
        assertThat(converted.getSpec().getTarget().getStatusStorageTopic(), is("my-mm2-status"));
        assertThat(converted.getSpec().getTarget().getConfig().get("group.id"), is(nullValue()));
        assertThat(converted.getSpec().getTarget().getConfig().get("config.storage.topic"), is(nullValue()));
        assertThat(converted.getSpec().getTarget().getConfig().get("offset.storage.topic"), is(nullValue()));
        assertThat(converted.getSpec().getTarget().getConfig().get("status.storage.topic"), is(nullValue()));
        assertThat(converted.getSpec().getMirrors().get(0).getSourceCluster(), is(nullValue()));
        assertThat(converted.getSpec().getMirrors().get(0).getTargetCluster(), is(nullValue()));
        assertThat(converted.getSpec().getMirrors().get(0).getSource().getAlias(), is("source"));
        assertThat(converted.getSpec().getMirrors().get(0).getSource().getBootstrapServers(), is("source:9092"));
    }
}