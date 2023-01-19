/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.Constants;
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
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "deployment", "pod", "apiService", "podDisruptionBudget", "cruiseControlContainer", "tlsSidecarContainer", "serviceAccount"})
@EqualsAndHashCode
public class CruiseControlTemplate implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private DeploymentTemplate deployment;
    private PodTemplate pod;
    private InternalServiceTemplate apiService;
    private PodDisruptionBudgetTemplate podDisruptionBudget;
    private ContainerTemplate cruiseControlContainer;
    private ContainerTemplate tlsSidecarContainer;
    private ResourceTemplate serviceAccount;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Template for Cruise Control `Deployment`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public DeploymentTemplate getDeployment() {
        return deployment;
    }

    public void setDeployment(DeploymentTemplate deployment) {
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
    public InternalServiceTemplate getApiService() {
        return apiService;
    }

    public void setApiService(InternalServiceTemplate apiService) {
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

    @DeprecatedProperty
    @Deprecated
    @Description("Template for the Cruise Control TLS sidecar container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getTlsSidecarContainer() {
        return tlsSidecarContainer;
    }

    public void setTlsSidecarContainer(ContainerTemplate tlsSidecarContainer) {
        this.tlsSidecarContainer = tlsSidecarContainer;
    }

    @Description("Template for the Cruise Control service account.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getServiceAccount() {
        return serviceAccount;
    }

    public void setServiceAccount(ResourceTemplate serviceAccount) {
        this.serviceAccount = serviceAccount;
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
