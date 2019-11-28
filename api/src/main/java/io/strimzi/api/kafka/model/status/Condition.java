/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
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
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "type", "status", "lastTransitionTime", "reason", "message" })
@EqualsAndHashCode
@ToString
public class Condition implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private String status;
    private String reason;
    private String message;
    private String type;
    private String lastTransitionTime;
    private Map<String, Object> additionalProperties;

    @Description("The status of the condition, either True, False or Unknown.")
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Description("The reason for the condition's last transition (a single word in CamelCase).")
    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    @Description("The unique identifier of a condition, used to distinguish between other conditions in the resource.")
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Description("Last time the condition of a type changed from one status to another. " +
            "The required format is 'yyyy-MM-ddTHH:mm:ssZ', in the UTC time zone")
    public String getLastTransitionTime() {
        return lastTransitionTime;
    }

    public void setLastTransitionTime(String lastTransitionTime) {
        this.lastTransitionTime = lastTransitionTime;
    }

    @Description("Human-readable message indicating details about the condition's last transition.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
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
