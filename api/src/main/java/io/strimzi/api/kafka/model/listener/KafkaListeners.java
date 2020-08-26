/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Configures the broker authorization
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"plain", "tls", "external"})
@EqualsAndHashCode
public class KafkaListeners implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private KafkaListenerTls tls;
    private KafkaListenerPlain plain;
    private KafkaListenerExternal external;
    private Map<String, Object> additionalProperties;

    @Description("Configures TLS listener on port 9093.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaListenerTls getTls() {
        return tls;
    }

    public void setTls(KafkaListenerTls tls) {
        this.tls = tls;
    }

    @Description("Configures plain listener on port 9092.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaListenerPlain getPlain() {
        return plain;
    }

    public void setPlain(KafkaListenerPlain plain) {
        this.plain = plain;
    }

    @Description("Configures external listener on port 9094.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaListenerExternal getExternal() {
        return external;
    }

    public void setExternal(KafkaListenerExternal external) {
        this.external = external;
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
