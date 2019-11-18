/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Configures External listeners
 */

@JsonSubTypes({
        @JsonSubTypes.Type(name = KafkaListenerExternalRoute.TYPE_ROUTE, value = RouteListenerConfiguration.class),
        @JsonSubTypes.Type(name = KafkaListenerExternalLoadBalancer.TYPE_LOADBALANCER, value = LoadBalancerListenerConfiguration.class),
        @JsonSubTypes.Type(name = KafkaListenerExternalNodePort.TYPE_NODEPORT, value = NodePortListenerConfiguration.class),
        @JsonSubTypes.Type(name = KafkaListenerExternalIngress.TYPE_INGRESS, value = IngressListenerConfiguration.class),
})
@JsonPropertyOrder({"serverKey"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    generateBuilderPackage = false,
    builderPackage = "io.fabric8.kubernetes.api.builder"
)
@EqualsAndHashCode
public abstract class KafkaExternalListenerConfiguration implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private CertAndKeySecretSource serverKey;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Reference to the `Secret` which holds the certificate and private key pair.")
    public CertAndKeySecretSource getServerKey() {
        return serverKey;
    }

    public void setServerKey(CertAndKeySecretSource serverKey) {
        this.serverKey = serverKey;
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
