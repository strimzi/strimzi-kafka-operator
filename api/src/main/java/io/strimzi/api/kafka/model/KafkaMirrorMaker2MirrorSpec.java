/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"sourceCluster", "targetCluster", "sourceConnector", "checkpointConnector", "heartbeatConnector", "topicsPattern", "topicsBlacklistPattern", "groupsPattern", "groupsBlacklistPattern"})
@EqualsAndHashCode
public class KafkaMirrorMaker2MirrorSpec implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private String sourceCluster;
    private String targetCluster;
    private KafkaMirrorMaker2ConnectorSpec sourceConnector;
    private KafkaMirrorMaker2ConnectorSpec checkpointConnector;
    private KafkaMirrorMaker2ConnectorSpec heartbeatConnector;
    private String topicsPattern;
    private String topicsBlacklistPattern;
    private String groupsPattern;
    private String groupsBlacklistPattern;
    private Map<String, Object> additionalProperties;

    @Description("A regular expression matching the topics to be mirrored, for example, \"topic1|topic2|topic3\". Comma-separated lists are also supported.")
    public String getTopicsPattern() {
        return topicsPattern;
    }

    public void setTopicsPattern(String topicsPattern) {
        this.topicsPattern = topicsPattern;
    }

    @Description("A regular expression matching the topics to exclude from mirroring. Comma-separated lists are also supported.")
    public String getTopicsBlacklistPattern() {
        return topicsBlacklistPattern;
    }

    public void setTopicsBlacklistPattern(String topicsBlacklistPattern) {
        this.topicsBlacklistPattern = topicsBlacklistPattern;
    }

    @Description("A regular expression matching the consumer groups to be mirrored. Comma-separated lists are also supported.")
    public String getGroupsPattern() {
        return groupsPattern;
    }

    public void setGroupsPattern(String groupsPattern) {
        this.groupsPattern = groupsPattern;
    }

    @Description("A regular expression matching the consumer groups to exclude from mirroring. Comma-separated lists are also supported.")
    public String getGroupsBlacklistPattern() {
        return groupsBlacklistPattern;
    }

    public void setGroupsBlacklistPattern(String groupsBlacklistPattern) {
        this.groupsBlacklistPattern = groupsBlacklistPattern;
    }

    @Description("The alias of the source cluster used by the Kafka MirrorMaker 2.0 connectors. The alias must match a cluster in the list at `spec.clusters`.")
    @JsonProperty(required = true)
    public String getSourceCluster() {
        return sourceCluster;
    }

    public void setSourceCluster(String sourceCluster) {
        this.sourceCluster = sourceCluster;
    }

    @Description("The alias of the target cluster used by the Kafka MirrorMaker 2.0 connectors. The alias must match a cluster in the list at `spec.clusters`.")
    @JsonProperty(required = true)
    public String getTargetCluster() {
        return targetCluster;
    }

    public void setTargetCluster(String targetCluster) {
        this.targetCluster = targetCluster;
    }

    @Description("The specification of the Kafka MirrorMaker 2.0 source connector.")
    public KafkaMirrorMaker2ConnectorSpec getSourceConnector() {
        return sourceConnector;
    }

    public void setSourceConnector(KafkaMirrorMaker2ConnectorSpec sourceConnector) {        
        this.sourceConnector = sourceConnector;
    }

    @Description("The specification of the Kafka MirrorMaker 2.0 checkpoint connector.")
    public KafkaMirrorMaker2ConnectorSpec getCheckpointConnector() {
        return checkpointConnector;
    }

    public void setCheckpointConnector(KafkaMirrorMaker2ConnectorSpec checkpointConnector) {
        this.checkpointConnector = checkpointConnector;
    }

    @Description("The specification of the Kafka MirrorMaker 2.0 heartbeat connector.")
    public KafkaMirrorMaker2ConnectorSpec getHeartbeatConnector() {
        return heartbeatConnector;
    }

    public void setHeartbeatConnector(KafkaMirrorMaker2ConnectorSpec heartbeatConnector) {
        this.heartbeatConnector = heartbeatConnector;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(1);
        }
        this.additionalProperties.put(name, value);
    }
}

