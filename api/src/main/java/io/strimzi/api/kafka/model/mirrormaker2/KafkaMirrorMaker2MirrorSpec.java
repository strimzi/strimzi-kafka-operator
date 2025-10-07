/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker2;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.strimzi.crdgenerator.annotations.RequiredInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"sourceCluster", "targetCluster", "source", "sourceConnector", "heartbeatConnector", "checkpointConnector",
    "topicsPattern", "topicsBlacklistPattern", "topicsExcludePattern", "groupsPattern", "groupsBlacklistPattern", "groupsExcludePattern"})
@EqualsAndHashCode
@ToString
public class KafkaMirrorMaker2MirrorSpec implements UnknownPropertyPreserving {
    private String sourceCluster;
    private String targetCluster;
    private KafkaMirrorMaker2ClusterSpec source;
    private KafkaMirrorMaker2ConnectorSpec sourceConnector;
    private KafkaMirrorMaker2ConnectorSpec checkpointConnector;
    private KafkaMirrorMaker2ConnectorSpec heartbeatConnector;
    private String topicsPattern;
    private String topicsBlacklistPattern;
    private String topicsExcludePattern;
    private String groupsPattern;
    private String groupsBlacklistPattern;
    private String groupsExcludePattern;
    private Map<String, Object> additionalProperties;

    @Description("The source Apache Kafka cluster. " +
            "The source Kafka cluster is used by the Kafka MirrorMaker 2 connectors.")
    @RequiredInVersions("v1+")
    public KafkaMirrorMaker2ClusterSpec getSource() {
        return source;
    }

    public void setSource(KafkaMirrorMaker2ClusterSpec source) {
        this.source = source;
    }

    @Description("A regular expression matching the topics to be mirrored, for example, \"topic1|topic2|topic3\". Comma-separated lists are also supported.")
    public String getTopicsPattern() {
        return topicsPattern;
    }

    public void setTopicsPattern(String topicsPattern) {
        this.topicsPattern = topicsPattern;
    }

    @DeprecatedProperty(movedToPath = ".spec.mirrors.topicsExcludePattern")
    @PresentInVersions("v1alpha1-v1beta2")
    @Deprecated
    @Description("A regular expression matching the topics to exclude from mirroring. Comma-separated lists are also supported.")
    public String getTopicsBlacklistPattern() {
        return topicsBlacklistPattern;
    }

    public void setTopicsBlacklistPattern(String topicsBlacklistPattern) {
        this.topicsBlacklistPattern = topicsBlacklistPattern;
    }

    @Description("A regular expression matching the topics to exclude from mirroring. Comma-separated lists are also supported.")
    public String getTopicsExcludePattern() {
        return topicsExcludePattern;
    }

    public void setTopicsExcludePattern(String topicsExcludePattern) {
        this.topicsExcludePattern = topicsExcludePattern;
    }

    @Description("A regular expression matching the consumer groups to be mirrored. Comma-separated lists are also supported.")
    public String getGroupsPattern() {
        return groupsPattern;
    }

    public void setGroupsPattern(String groupsPattern) {
        this.groupsPattern = groupsPattern;
    }

    @DeprecatedProperty(movedToPath = ".spec.mirrors.groupsExcludePattern")
    @PresentInVersions("v1alpha1-v1beta2")
    @Deprecated
    @Description("A regular expression matching the consumer groups to exclude from mirroring. Comma-separated lists are also supported.")
    public String getGroupsBlacklistPattern() {
        return groupsBlacklistPattern;
    }

    public void setGroupsBlacklistPattern(String groupsBlacklistPattern) {
        this.groupsBlacklistPattern = groupsBlacklistPattern;
    }

    @Description("A regular expression matching the consumer groups to exclude from mirroring. Comma-separated lists are also supported.")
    public String getGroupsExcludePattern() {
        return groupsExcludePattern;
    }

    public void setGroupsExcludePattern(String groupsExcludePattern) {
        this.groupsExcludePattern = groupsExcludePattern;
    }

    @Description("The alias of the source cluster used by the Kafka MirrorMaker 2 connectors. The alias must match a cluster in the list at `spec.clusters`.")
    @Deprecated
    @DeprecatedProperty(movedToPath = ".spec.mirrors.source", description = "The `sourceCluster` field is deprecated and will be removed in the `v1` CRD API.")
    @PresentInVersions("v1beta2")
    public String getSourceCluster() {
        return sourceCluster;
    }

    public void setSourceCluster(String sourceCluster) {
        this.sourceCluster = sourceCluster;
    }

    @Description("The alias of the target cluster used by the Kafka MirrorMaker 2 connectors. The alias must match a cluster in the list at `spec.clusters`.")
    @Deprecated
    @DeprecatedProperty(movedToPath = ".spec.target", description = "The `targetCluster` field is deprecated and will be removed in the `v1` CRD API.")
    @PresentInVersions("v1beta2")
    public String getTargetCluster() {
        return targetCluster;
    }

    public void setTargetCluster(String targetCluster) {
        this.targetCluster = targetCluster;
    }

    @Description("The specification of the Kafka MirrorMaker 2 source connector.")
    public KafkaMirrorMaker2ConnectorSpec getSourceConnector() {
        return sourceConnector;
    }

    public void setSourceConnector(KafkaMirrorMaker2ConnectorSpec sourceConnector) {
        this.sourceConnector = sourceConnector;
    }

    @Description("The specification of the Kafka MirrorMaker 2 checkpoint connector.")
    public KafkaMirrorMaker2ConnectorSpec getCheckpointConnector() {
        return checkpointConnector;
    }

    public void setCheckpointConnector(KafkaMirrorMaker2ConnectorSpec checkpointConnector) {
        this.checkpointConnector = checkpointConnector;
    }

    @Description("The specification of the Kafka MirrorMaker 2 heartbeat connector.")
    @Deprecated
    @DeprecatedProperty(description = "The `heartbeatConnector` is deprecated and will be removed in the `v1` CRD API.")
    @PresentInVersions("v1beta2")
    public KafkaMirrorMaker2ConnectorSpec getHeartbeatConnector() {
        return heartbeatConnector;
    }

    public void setHeartbeatConnector(KafkaMirrorMaker2ConnectorSpec heartbeatConnector) {
        this.heartbeatConnector = heartbeatConnector;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}

