/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker2;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.connector.AutoRestartStatus;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Represents a status of the Kafka MirrorMaker 2 resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "conditions", "observedGeneration", "url", "connectors", "autoRestartStatuses",
    "connectorPlugins", "labelSelector", "replicas" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaMirrorMaker2Status extends KafkaConnectStatus {
    private List<Map<String, Object>> connectors = new ArrayList<>(3);

    private List<AutoRestartStatus> autoRestartStatuses = new ArrayList<>();

    @Description("List of MirrorMaker 2 connector statuses, as reported by the Kafka Connect REST API.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<Map<String, Object>> getConnectors() {
        return connectors;
    }

    public void setConnectors(List<Map<String, Object>> connectors) {
        this.connectors = connectors;
    }

    /**
     * Synchronized method for adding a connector to the status. It adds the connector and sorts the list to make sure
     * that when the status is not changing all the time and triggering reconciliations in a loop.
     *
     * @param connector The connector status
     */
    public synchronized void addConnector(Map<String, Object> connector) {
        this.connectors.add(connector);
        this.connectors.sort(new ConnectorsComparatorByName());
    }

    @Description("List of MirrorMaker 2 connector auto restart statuses")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<AutoRestartStatus> getAutoRestartStatuses() {
        return autoRestartStatuses;
    }

    public void setAutoRestartStatuses(List<AutoRestartStatus> autoRestartStatuses) {
        this.autoRestartStatuses = autoRestartStatuses;
    }

    /**
     * Synchronized method for adding an auto-restart to the status. It adds the connector and sorts the list to make sure
     * that when the status is not changing all the time and triggering reconciliations in a loop.
     *
     * @param status    The Auto-restart status
     */
    public synchronized void addAutoRestartStatus(AutoRestartStatus status) {
        this.autoRestartStatuses.add(status);
        this.autoRestartStatuses.sort(Comparator.comparing(AutoRestartStatus::getConnectorName));
    }

    /**
     * This comparator compares two maps where connectors' configurations are stored.
     * The comparison is done by using only one property - 'name'
     */
    private static class ConnectorsComparatorByName implements Comparator<Map<String, Object>>, Serializable {
        private static final long serialVersionUID = 1L;
        
        @Override
        public int compare(Map<String, Object> m1, Map<String, Object> m2) {
            String name1 = m1.get("name") == null ? "" : m1.get("name").toString();
            String name2 = m2.get("name") == null ? "" : m2.get("name").toString();
            return name1.compareTo(name2);
        }
    }
}