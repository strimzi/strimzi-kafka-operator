/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configures external listener overrides for bootstrap service
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode
public class ExternalListenerBootstrapOverride implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private String address;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Additional address name for the bootstrap service. " +
            "The address will be added to the list of subject alternative names of the TLS certificates.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
