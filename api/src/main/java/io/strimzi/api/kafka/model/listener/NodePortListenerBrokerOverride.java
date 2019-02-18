/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

/**
 * Configures external broker service and advertised addresses for NodePort listeners
 */
@JsonPropertyOrder({"broker", "advertisedHost", "advertisedPort", "nodePort"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    generateBuilderPackage = false,
    builderPackage = "io.fabric8.kubernetes.api.builder"
)
@EqualsAndHashCode(callSuper = true)
public class NodePortListenerBrokerOverride extends ExternalListenerBrokerOverride {
    private static final long serialVersionUID = 1L;

    private Integer nodePort;

    @Description("Node port for the broker service")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getNodePort() {
        return nodePort;
    }

    public void setNodePort(Integer nodePort) {
        this.nodePort = nodePort;
    }
}
