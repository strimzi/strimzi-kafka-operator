/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

/**
 * Configures external broker service and advertised addresses for NodePort listeners
 */
@JsonPropertyOrder({"broker", "advertisedHost", "advertisedPort", "nodePort"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode(callSuper = true)
public class NodePortListenerBrokerOverride extends ExternalListenerBrokerOverride {
    private static final long serialVersionUID = 1L;

    private Integer nodePort;
    private Map<String, String> dnsAnnotations = new HashMap<>(0);

    @Description("Node port for the broker service")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getNodePort() {
        return nodePort;
    }

    public void setNodePort(Integer nodePort) {
        this.nodePort = nodePort;
    }

    @Description("Annotations that will be added to the `Service` resources for individual brokers. " +
            "You can use this field to configure DNS providers such as External DNS.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> getDnsAnnotations() {
        return dnsAnnotations;
    }

    public void setDnsAnnotations(Map<String, String> dnsAnnotations) {
        this.dnsAnnotations = dnsAnnotations;
    }
}
