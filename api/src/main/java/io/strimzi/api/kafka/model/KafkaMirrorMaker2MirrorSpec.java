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
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"sourceCluster", "targetCluster", "sourceConnector", "checkpointConnector", "heartbeatConnector", "topics", "groups"})
@EqualsAndHashCode
public class KafkaMirrorMaker2MirrorSpec implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private String sourceCluster;
    private String targetCluster;
    private KafkaConnectorSpec sourceConnector;
    private KafkaConnectorSpec checkpointConnector;
    private KafkaConnectorSpec heartbeatConnector;
    private String topics;
    private String groups;
    private Map<String, Object> additionalProperties;

    @Description("Regex of topics mirrored by the Kafka Mirror Maker 2 connectors")
    @JsonProperty(required = true)
    public String getTopics() {
        return topics;
    }

    public void setTopics(String topics) {
        this.topics = topics;
    }

    @Description("Regex of consumer groups mirrored by the Kafka Mirror Maker 2 connectors")
    public String getGroups() {
        return groups;
    }

    public void setGroups(String groups) {
        this.groups = groups;
    }

    @Description("The source cluster used by the Kafka Mirror Maker 2 connectors")
    @JsonProperty(required = true)
    public String getSourceCluster() {
        return sourceCluster;
    }

    public void setSourceCluster(String sourceCluster) {
        this.sourceCluster = sourceCluster;
    }

    @Description("The target cluster used by the Kafka Mirror Maker 2 connectors")
    @JsonProperty(required = true)
    public String getTargetCluster() {
        return targetCluster;
    }

    public void setTargetCluster(String targetCluster) {
        this.targetCluster = targetCluster;
    }

    @Description("The specification of the Kafka Mirror Maker 2 source connector")
    public KafkaConnectorSpec getSourceConnector() {
        return sourceConnector;
    }

    public void setSourceConnector(KafkaConnectorSpec sourceConnector) {        
        this.sourceConnector = sourceConnector;
        if (this.sourceConnector != null) {
            this.sourceConnector.setClassName("org.apache.kafka.connect.mirror.MirrorSourceConnector");
        }
    }

    @Description("The specification of the Kafka Mirror Maker 2 checkpoint connector")
    public KafkaConnectorSpec getCheckpointConnector() {
        return checkpointConnector;
    }

    public void setCheckpointConnector(KafkaConnectorSpec checkpointConnector) {
        this.checkpointConnector = checkpointConnector;
        if (this.checkpointConnector != null) {
            this.checkpointConnector.setClassName("org.apache.kafka.connect.mirror.MirrorCheckpointConnector");
        }
    }

    @Description("The specification of the Kafka Mirror Maker 2 heartbeat connector")
    public KafkaConnectorSpec getHeartbeatConnector() {
        return heartbeatConnector;
    }

    public void setHeartbeatConnector(KafkaConnectorSpec heartbeatConnector) {
        this.heartbeatConnector = heartbeatConnector;
        if (this.heartbeatConnector != null) {
            this.heartbeatConnector.setClassName("org.apache.kafka.connect.mirror.MirrorHeartbeatConnector");
        }
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>();
        }
        this.additionalProperties.put(name, value);
    }
}

