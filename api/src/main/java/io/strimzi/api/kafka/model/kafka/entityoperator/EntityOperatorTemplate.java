/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.entityoperator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.common.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a template for Entity Operator resources.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"deployment", "pod", "topicOperatorContainer", "userOperatorContainer", "tlsSidecarContainer", "serviceAccount", "podDisruptionBudget", "entityOperatorRole", "topicOperatorRoleBinding", "userOperatorRoleBinding"})
@EqualsAndHashCode
@ToString
public class EntityOperatorTemplate implements UnknownPropertyPreserving {
    private DeploymentTemplate deployment;
    private PodTemplate pod;
    private ResourceTemplate entityOperatorRole;
    private ResourceTemplate topicOperatorRoleBinding;
    private ResourceTemplate userOperatorRoleBinding;
    private ContainerTemplate topicOperatorContainer;
    private ContainerTemplate userOperatorContainer;
    private ContainerTemplate tlsSidecarContainer;
    private ResourceTemplate serviceAccount;
    private PodDisruptionBudgetTemplate podDisruptionBudget;
    private Map<String, Object> additionalProperties;

    @Description("Template for Entity Operator `Deployment`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public DeploymentTemplate getDeployment() {
        return deployment;
    }

    public void setDeployment(DeploymentTemplate deployment) {
        this.deployment = deployment;
    }

    @Description("Template for Entity Operator `Pods`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodTemplate getPod() {
        return pod;
    }

    public void setPod(PodTemplate pod) {
        this.pod = pod;
    }

    @Description("Template for the Entity Operator Role")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getEntityOperatorRole() {
        return entityOperatorRole;
    }

    public void setEntityOperatorRole(ResourceTemplate entityOperatorRole) {
        this.entityOperatorRole = entityOperatorRole;
    }

    @Description("Template for the Entity Topic Operator RoleBinding")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getTopicOperatorRoleBinding() {
        return topicOperatorRoleBinding;
    }

    public void setTopicOperatorRoleBinding(ResourceTemplate topicOperatorRoleBinding) {
        this.topicOperatorRoleBinding = topicOperatorRoleBinding;
    }

    @Description("Template for the Entity Topic Operator RoleBinding")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getUserOperatorRoleBinding() {
        return userOperatorRoleBinding;
    }

    public void setUserOperatorRoleBinding(ResourceTemplate userOperatorRoleBinding) {
        this.userOperatorRoleBinding = userOperatorRoleBinding;
    }

    @Description("Template for the Entity Topic Operator container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getTopicOperatorContainer() {
        return topicOperatorContainer;
    }

    public void setTopicOperatorContainer(ContainerTemplate topicOperatorContainer) {
        this.topicOperatorContainer = topicOperatorContainer;
    }

    @Description("Template for the Entity User Operator container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getUserOperatorContainer() {
        return userOperatorContainer;
    }

    public void setUserOperatorContainer(ContainerTemplate userOperatorContainer) {
        this.userOperatorContainer = userOperatorContainer;
    }

    @Deprecated
    @DeprecatedProperty(description = "TLS sidecar was removed in Strimzi 0.41.0. This property is ignored.")
    @Description("Template for the Entity Operator TLS sidecar container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getTlsSidecarContainer() {
        return tlsSidecarContainer;
    }

    public void setTlsSidecarContainer(ContainerTemplate tlsSidecarContainer) {
        this.tlsSidecarContainer = tlsSidecarContainer;
    }

    @Description("Template for the Entity Operator service account.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getServiceAccount() {
        return serviceAccount;
    }

    public void setServiceAccount(ResourceTemplate serviceAccount) {
        this.serviceAccount = serviceAccount;
    }

    @Description("Template for the Entity Operator Pod Disruption Budget.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodDisruptionBudgetTemplate getPodDisruptionBudget() {
        return podDisruptionBudget;
    }
    public void setPodDisruptionBudget(PodDisruptionBudgetTemplate podDisruptionBudget) {
        this.podDisruptionBudget = podDisruptionBudget;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
