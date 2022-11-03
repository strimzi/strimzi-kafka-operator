/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "count", "connectorName", "lastRestartTimestamp"})
@EqualsAndHashCode
@ToString(callSuper = true)
public class AutoRestartStatus implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private int count;

    private String connectorName;

    private String lastRestartTimestamp;
    private Map<String, Object> additionalProperties;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The number of times the connector or task is restarted.")
    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The name of the connector being restarted.")
    public String getConnectorName() {
        return connectorName;
    }

    public void setConnectorName(String connectorName) {
        this.connectorName = connectorName;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The last time the automatic restart was attempted. " +
        "The required format is 'yyyy-MM-ddTHH:mm:ssZ' in the UTC time zone.")
    public String getLastRestartTimestamp() {
        return lastRestartTimestamp;
    }

    public void setLastRestartTimestamp(String lastRestartTimestamp) {
        this.lastRestartTimestamp = lastRestartTimestamp;
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
