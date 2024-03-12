/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

/**
 * Represents a status of the Kafka resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "conditions", "observedGeneration", "listeners", "kafkaNodePools", "clusterId", "operatorLastSuccessfulVersion", "kafkaVersion", "kafkaMetadataVersion", "kafkaMetadataState" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaStatus extends Status {
    private static final long serialVersionUID = 1L;

    private List<ListenerStatus> listeners;
    private List<UsedNodePoolStatus> kafkaNodePools;
    
    private String clusterId;
    private String operatorLastSuccessfulVersion;
    private String kafkaVersion;
    private String kafkaMetadataVersion;
    private KafkaMetadataState kafkaMetadataState;

    @Description("Addresses of the internal and external listeners")
    public List<ListenerStatus> getListeners() {
        return listeners;
    }

    public void setListeners(List<ListenerStatus> listeners) {
        this.listeners = listeners;
    }

    @Description("List of the KafkaNodePools used by this Kafka cluster")
    public List<UsedNodePoolStatus> getKafkaNodePools() {
        return kafkaNodePools;
    }

    public void setKafkaNodePools(List<UsedNodePoolStatus> kafkaNodePools) {
        this.kafkaNodePools = kafkaNodePools;
    }

    @Description("Kafka cluster Id")
    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    @Description("The version of the Strimzi Cluster Operator which performed the last successful reconciliation.")
    public String getOperatorLastSuccessfulVersion() {
        return operatorLastSuccessfulVersion;
    }

    public void setOperatorLastSuccessfulVersion(String operatorLastSuccessfulVersion) {
        this.operatorLastSuccessfulVersion = operatorLastSuccessfulVersion;
    }

    @Description("The version of Kafka currently deployed in the cluster.")
    public String getKafkaVersion() {
        return kafkaVersion;
    }

    public void setKafkaVersion(String kafkaVersion) {
        this.kafkaVersion = kafkaVersion;
    }

    @Description("The KRaft metadata.version currently used by the Kafka cluster.")
    public String getKafkaMetadataVersion() {
        return kafkaMetadataVersion;
    }

    public void setKafkaMetadataVersion(String kafkaMetadataVersion) {
        this.kafkaMetadataVersion = kafkaMetadataVersion;
    }

    @Description("Defines where cluster metadata are stored. Possible values are: " +
            "ZooKeeper if the metadata are stored in ZooKeeper; " +
            "KRaftMigration if the controllers are connected to ZooKeeper, brokers are being rolled with Zookeeper migration enabled and connection information to controllers, and the metadata migration process is running; " +
            "KRaftDualWriting if the metadata migration process finished and the cluster is in dual-write mode; " +
            "KRaftPostMigration if the brokers are fully KRaft-based but controllers being rolled to disconnect from ZooKeeper; " +
            "PreKRaft if brokers and controller are fully KRaft-based, metadata are stored in KRaft, but ZooKeeper must be deleted; " +
            "KRaft if the metadata are stored in KRaft.")
    public KafkaMetadataState getKafkaMetadataState() {
        return kafkaMetadataState;
    }

    public void setKafkaMetadataState(KafkaMetadataState metadataState) {
        this.kafkaMetadataState = metadataState;
    }
}
