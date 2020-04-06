/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

/**
 * Configures broker ingress
 */
@JsonPropertyOrder({"broker", "advertisedHost", "advertisedPort", "host"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode(callSuper = true)
public class IngressListenerBrokerConfiguration extends ExternalListenerBrokerOverride {
    private static final long serialVersionUID = 1L;

    private String host;
    private Map<String, String> dnsAnnotations = new HashMap<>(0);

    @Description("Host for the broker ingress. " +
            "This field will be used in the Ingress resource.")
    @JsonProperty(required = true)
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Description("Annotations that will be added to the `Ingress` resources for individual brokers. " +
            "You can use this field to configure DNS providers such as External DNS.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> getDnsAnnotations() {
        return dnsAnnotations;
    }

    public void setDnsAnnotations(Map<String, String> dnsAnnotations) {
        this.dnsAnnotations = dnsAnnotations;
    }
}
