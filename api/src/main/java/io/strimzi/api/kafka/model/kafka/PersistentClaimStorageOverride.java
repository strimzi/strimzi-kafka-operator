/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Configures persistent claim overrides for storage - allows to override storage class with per broker configuration
 */
@JsonPropertyOrder({"class", "broker"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode
@ToString
public class PersistentClaimStorageOverride  implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private Integer broker;
    private String storageClass;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Id of the kafka broker (broker identifier)")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getBroker() {
        return broker;
    }

    public void setBroker(Integer broker) {
        this.broker = broker;
    }

    @JsonProperty("class")
    @Description("The storage class to use for dynamic volume allocation for this broker.")
    public String getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
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
