/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Configures Ingress listeners
 */
@JsonPropertyOrder({"bootstrap", "brokers"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    generateBuilderPackage = false,
    builderPackage = "io.fabric8.kubernetes.api.builder"
)
@EqualsAndHashCode
public class IngressListenerConfiguration implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private IngressListenerBootstrapConfiguration bootstrap;
    private List<IngressListenerBrokerConfiguration> brokers;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("External bootstrap ingress configuration")
    public IngressListenerBootstrapConfiguration getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(IngressListenerBootstrapConfiguration bootstrap) {
        this.bootstrap = bootstrap;
    }

    @Description("External broker ingress configuration")
    public List<IngressListenerBrokerConfiguration> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<IngressListenerBrokerConfiguration> brokers) {
        this.brokers = brokers;
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
