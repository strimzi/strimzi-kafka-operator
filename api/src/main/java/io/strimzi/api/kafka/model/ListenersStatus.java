/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
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
@JsonPropertyOrder({ "plain", "tls", "external"})
@EqualsAndHashCode
public class ListenersStatus implements UnknownPropertyPreserving, Serializable {
    private ListenerStatus plain;
    private ListenerStatus tls;
    private ListenerStatus external;
    private Map<String, Object> additionalProperties;

    public ListenerStatus getPlain() {
        return plain;
    }

    public void setPlain(ListenerStatus plain) {
        this.plain = plain;
    }

    public ListenerStatus getTls() {
        return tls;
    }

    public void setTls(ListenerStatus tls) {
        this.tls = tls;
    }

    public ListenerStatus getExternal() {
        return external;
    }

    public void setExternal(ListenerStatus external) {
        this.external = external;
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
