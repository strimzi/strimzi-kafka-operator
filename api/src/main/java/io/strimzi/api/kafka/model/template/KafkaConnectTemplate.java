/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a template for Kafka Connect resources.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"deployment", "pod", "apiService", "connectContainer", "initContainer", "podDisruptionBudget",
        "serviceAccount", "clusterRoleBinding", "buildPod", "buildContainer", "buildConfig", "buildServiceAccount"})
@EqualsAndHashCode
public class KafkaConnectTemplate implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private DeploymentTemplate deployment;
    private PodTemplate pod;
    private PodTemplate buildPod;
    private InternalServiceTemplate apiService;
    private PodDisruptionBudgetTemplate podDisruptionBudget;
    private ContainerTemplate connectContainer;
    private ContainerTemplate initContainer;
    private ContainerTemplate buildContainer;
    private ResourceTemplate buildConfig;
    private ResourceTemplate clusterRoleBinding;
    private ResourceTemplate serviceAccount;
    private ResourceTemplate buildServiceAccount;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Template for Kafka Connect `Deployment`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public DeploymentTemplate getDeployment() {
        return deployment;
    }

    public void setDeployment(DeploymentTemplate deployment) {
        this.deployment = deployment;
    }

    @Description("Template for Kafka Connect `Pods`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodTemplate getPod() {
        return pod;
    }

    public void setPod(PodTemplate pod) {
        this.pod = pod;
    }

    @Description("Template for Kafka Connect Build `Pods`. " +
            "The build pod is used only on Kubernetes.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodTemplate getBuildPod() {
        return buildPod;
    }

    public void setBuildPod(PodTemplate buildPod) {
        this.buildPod = buildPod;
    }

    @Description("Template for Kafka Connect API `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public InternalServiceTemplate getApiService() {
        return apiService;
    }

    public void setApiService(InternalServiceTemplate apiService) {
        this.apiService = apiService;
    }

    @Description("Template for Kafka Connect `PodDisruptionBudget`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodDisruptionBudgetTemplate getPodDisruptionBudget() {
        return podDisruptionBudget;
    }

    public void setPodDisruptionBudget(PodDisruptionBudgetTemplate podDisruptionBudget) {
        this.podDisruptionBudget = podDisruptionBudget;
    }

    @Description("Template for the Kafka Connect container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getConnectContainer() {
        return connectContainer;
    }

    public void setConnectContainer(ContainerTemplate connectContainer) {
        this.connectContainer = connectContainer;
    }

    @Description("Template for the Kafka init container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getInitContainer() {
        return initContainer;
    }

    public void setInitContainer(ContainerTemplate initContainer) {
        this.initContainer = initContainer;
    }

    @Description("Template for the Kafka Connect Build container. " +
            "The build container is used only on Kubernetes.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getBuildContainer() {
        return buildContainer;
    }

    public void setBuildContainer(ContainerTemplate buildContainer) {
        this.buildContainer = buildContainer;
    }

    @Description("Template for the Kafka Connect BuildConfig used to build new container images. " +
            "The BuildConfig is used only on OpenShift.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getBuildConfig() {
        return buildConfig;
    }

    public void setBuildConfig(ResourceTemplate buildConfig) {
        this.buildConfig = buildConfig;
    }

    @Description("Template for the Kafka Connect ClusterRoleBinding.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getClusterRoleBinding() {
        return clusterRoleBinding;
    }

    public void setClusterRoleBinding(ResourceTemplate clusterRoleBinding) {
        this.clusterRoleBinding = clusterRoleBinding;
    }

    @Description("Template for the Kafka Connect service account.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getServiceAccount() {
        return serviceAccount;
    }

    public void setServiceAccount(ResourceTemplate serviceAccount) {
        this.serviceAccount = serviceAccount;
    }

    @Description("Template for the Kafka Connect Build service account.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getBuildServiceAccount() {
        return buildServiceAccount;
    }

    public void setBuildServiceAccount(ResourceTemplate buildServiceAccount) {
        this.buildServiceAccount = buildServiceAccount;
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
