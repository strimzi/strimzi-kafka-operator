/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;

/**
 * Representation of a template for Zookeeper cluster resources.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "statefulset", "pod", "clientService", "nodesService"})
public class ZookeeperClusterTemplate implements Serializable {
    private static final long serialVersionUID = 1L;

    private StatefulSetTemplate statefulset;
    private PodTemplate pod;
    private ServiceTemplate clientService;
    private ServiceTemplate nodesService;

    @Description("Template for Zookeeper `StatefulSet`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public StatefulSetTemplate getStatefulset() {
        return statefulset;
    }

    public void setStatefulset(StatefulSetTemplate statefulset) {
        this.statefulset = statefulset;
    }

    @Description("Template for Zookeeper `Pods`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodTemplate getPod() {
        return pod;
    }

    public void setPod(PodTemplate pod) {
        this.pod = pod;
    }

    @Description("Template for Zookeeper client `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ServiceTemplate getClientService() {
        return clientService;
    }

    public void setClientService(ServiceTemplate clientService) {
        this.clientService = clientService;
    }

    @Description("Template for Zookeeper nodes `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ServiceTemplate getNodesService() {
        return nodesService;
    }

    public void setNodesService(ServiceTemplate nodesService) {
        this.nodesService = nodesService;
    }
}
