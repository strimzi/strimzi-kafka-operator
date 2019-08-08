/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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
@EqualsAndHashCode
public class ZookeeperClusterTemplate implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private ResourceTemplate statefulset;
    private PodTemplate pod;
    private ResourceTemplate clientService;
    private ResourceTemplate nodesService;
    private PodDisruptionBudgetTemplate podDisruptionBudget;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Template for Zookeeper `StatefulSet`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getStatefulset() {
        return statefulset;
    }

    public void setStatefulset(ResourceTemplate statefulset) {
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
    public ResourceTemplate getClientService() {
        return clientService;
    }

    public void setClientService(ResourceTemplate clientService) {
        this.clientService = clientService;
    }

    @Description("Template for Zookeeper nodes `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getNodesService() {
        return nodesService;
    }

    public void setNodesService(ResourceTemplate nodesService) {
        this.nodesService = nodesService;
    }

    @Description("Template for Zookeeper `PodDisruptionBudget`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodDisruptionBudgetTemplate getPodDisruptionBudget() {
        return podDisruptionBudget;
    }

    public void setPodDisruptionBudget(PodDisruptionBudgetTemplate podDisruptionBudget) {
        this.podDisruptionBudget = podDisruptionBudget;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
