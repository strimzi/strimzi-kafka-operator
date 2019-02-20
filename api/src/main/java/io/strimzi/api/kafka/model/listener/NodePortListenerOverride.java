/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.List;

/**
 * Configures overrides NodePort listeners
 */
@JsonPropertyOrder({"bootstrap", "brokers"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    generateBuilderPackage = false,
    builderPackage = "io.fabric8.kubernetes.api.builder"
)
public class NodePortListenerOverride implements Serializable {
    private static final long serialVersionUID = 1L;

    private NodePortListenerBootstrapOverride bootstrap;
    private List<NodePortListenerBrokerOverride> brokers;

    @Description("External bootstrap service configuration")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public NodePortListenerBootstrapOverride getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(NodePortListenerBootstrapOverride bootstrap) {
        this.bootstrap = bootstrap;
    }

    @Description("External broker services configuration")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<NodePortListenerBrokerOverride> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<NodePortListenerBrokerOverride> brokers) {
        this.brokers = brokers;
    }
}
