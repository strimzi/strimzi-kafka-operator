/*
 * Copyright Strimzi authors.
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
 * Representation of a template for Cruise Control resources.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "deployment", "pod", "apiService", "podDisruptionBudget", "cruiseControlContainer", "tlsSidecarContainer"})
@EqualsAndHashCode
public class CruiseControlTemplate implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private ResourceTemplate deployment;
    private PodTemplate pod;
    private ResourceTemplate apiService;
    private PodDisruptionBudgetTemplate podDisruptionBudget;
    private ContainerTemplate cruiseControlContainer;
    private ContainerTemplate tlsSidecarContainer;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Template for Cruise Control `Deployment`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getDeployment() {
        return deployment;
    }

    public void setDeployment(ResourceTemplate deployment) {
        this.deployment = deployment;
    }

    @Description("Template for Cruise Control `Pods`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodTemplate getPod() {
        return pod;
    }

    public void setPod(PodTemplate pod) {
        this.pod = pod;
    }

    @Description("Template for Cruise Control API `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getApiService() {
        return apiService;
    }

    public void setApiService(ResourceTemplate apiService) {
        this.apiService = apiService;
    }

    @Description("Template for Cruise Control `PodDisruptionBudget`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodDisruptionBudgetTemplate getPodDisruptionBudget() {
        return podDisruptionBudget;
    }

    public void setPodDisruptionBudget(PodDisruptionBudgetTemplate podDisruptionBudget) {
        this.podDisruptionBudget = podDisruptionBudget;
    }

    @Description("Template for the Cruise Control container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getCruiseControlContainer() {
        return cruiseControlContainer;
    }

    public void setCruiseControlContainer(ContainerTemplate cruiseControlContainer) {
        this.cruiseControlContainer = cruiseControlContainer;
    }

    @Description("Template for the Cruise Control TLS sidecar container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getTlsSidecarContainer() {
        return tlsSidecarContainer;
    }

    public void setTlsSidecarContainer(ContainerTemplate tlsSidecarContainer) {
        this.tlsSidecarContainer = tlsSidecarContainer;
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
