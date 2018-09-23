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
        "statefulset", "pods", "service", "headlessService"})
public class ZookeeperClusterTemplate implements Serializable {
    private static final long serialVersionUID = 1L;

    private StatefulSetTemplate statefulset;
    private PodTemplate pods;
    private ServiceTemplate service;
    private ServiceTemplate headlessService;

    @Description("Template for Zookeeper stateful set.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public StatefulSetTemplate getStatefulset() {
        return statefulset;
    }

    public void setStatefulset(StatefulSetTemplate statefulset) {
        this.statefulset = statefulset;
    }

    @Description("Template for Zookeeper pods.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodTemplate getPods() {
        return pods;
    }

    public void setPods(PodTemplate pods) {
        this.pods = pods;
    }

    @Description("Template for Zookeeper client service.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ServiceTemplate getService() {
        return service;
    }

    public void setService(ServiceTemplate service) {
        this.service = service;
    }

    @Description("Template for Zookeeper headless service.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ServiceTemplate getHeadlessService() {
        return headlessService;
    }

    public void setHeadlessService(ServiceTemplate headlessService) {
        this.headlessService = headlessService;
    }
}
