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
 * Representation of a template for Entity Operator resources.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"deployment", "pod", "topicOperatorContainer", "userOperatorContainer", "tlsSidecarContainer", "serviceAccount", "entityOperatorRole", "topicOperatorRoleBinding", "userOperatorRoleBinding"})
@EqualsAndHashCode
public class EntityOperatorTemplate implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;
    private DeploymentTemplate deployment;
    private PodTemplate pod;
    private ResourceTemplate entityOperatorRole;
    private ResourceTemplate topicOperatorRoleBinding;
    private ResourceTemplate userOperatorRoleBinding;
    private ContainerTemplate topicOperatorContainer;
    private ContainerTemplate userOperatorContainer;
    private ContainerTemplate tlsSidecarContainer;
    private ResourceTemplate serviceAccount;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

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

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
