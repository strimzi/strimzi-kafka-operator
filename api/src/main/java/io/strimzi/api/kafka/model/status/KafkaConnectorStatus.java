/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Map;

/**
 * Represents a status of the KafkaConnector resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "conditions", "observedGeneration", "url" })
@EqualsAndHashCode
@ToString(callSuper = true)
public class KafkaConnectorStatus extends Status {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> connectorStatus;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The connector status, as reported by the Kafka Connect REST API.")
    public Map<String, Object> getConnectorStatus() {
        return connectorStatus;
    }

    public void setConnectorStatus(Map<String, Object> connectorStatus) {
        this.connectorStatus = connectorStatus;
    }
}
