/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.zookeeper;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.HasJmxSecretTemplate;
import io.strimzi.api.kafka.model.common.template.InternalServiceTemplate;
import io.strimzi.api.kafka.model.common.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.common.template.StatefulSetTemplate;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a template for ZooKeeper cluster resources.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "statefulset", "podSet", "pod", "clientService", "nodesService", "persistentVolumeClaim",
    "podDisruptionBudget", "zookeeperContainer", "serviceAccount", "jmxSecret"})
@EqualsAndHashCode
@ToString
public class ZookeeperClusterTemplate implements HasJmxSecretTemplate, Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private StatefulSetTemplate statefulset;
    private ResourceTemplate podSet;
    private PodTemplate pod;
    private InternalServiceTemplate clientService;
    private InternalServiceTemplate nodesService;
    private ResourceTemplate persistentVolumeClaim;
    private PodDisruptionBudgetTemplate podDisruptionBudget;
    private ContainerTemplate zookeeperContainer;
    private ResourceTemplate serviceAccount;
    private ResourceTemplate jmxSecret;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Template for ZooKeeper `StatefulSet`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Deprecated
    @DeprecatedProperty(description = "Support for StatefulSets was removed in Strimzi 0.35.0. This property is ignored.")
    public StatefulSetTemplate getStatefulset() {
        return statefulset;
    }

    public void setStatefulset(StatefulSetTemplate statefulset) {
        this.statefulset = statefulset;
    }

    @Description("Template for ZooKeeper `StrimziPodSet` resource.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getPodSet() {
        return podSet;
    }

    public void setPodSet(ResourceTemplate podSetTemplate) {
        this.podSet = podSetTemplate;
    }

    @Description("Template for ZooKeeper `Pods`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodTemplate getPod() {
        return pod;
    }

    public void setPod(PodTemplate pod) {
        this.pod = pod;
    }

    @Description("Template for ZooKeeper client `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public InternalServiceTemplate getClientService() {
        return clientService;
    }

    public void setClientService(InternalServiceTemplate clientService) {
        this.clientService = clientService;
    }

    @Description("Template for ZooKeeper nodes `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public InternalServiceTemplate getNodesService() {
        return nodesService;
    }

    public void setNodesService(InternalServiceTemplate nodesService) {
        this.nodesService = nodesService;
    }

    @Description("Template for all ZooKeeper `PersistentVolumeClaims`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getPersistentVolumeClaim() {
        return persistentVolumeClaim;
    }

    public void setPersistentVolumeClaim(ResourceTemplate persistentVolumeClaim) {
        this.persistentVolumeClaim = persistentVolumeClaim;
    }

    @Description("Template for ZooKeeper `PodDisruptionBudget`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodDisruptionBudgetTemplate getPodDisruptionBudget() {
        return podDisruptionBudget;
    }

    public void setPodDisruptionBudget(PodDisruptionBudgetTemplate podDisruptionBudget) {
        this.podDisruptionBudget = podDisruptionBudget;
    }

    @Description("Template for the ZooKeeper container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getZookeeperContainer() {
        return zookeeperContainer;
    }

    public void setZookeeperContainer(ContainerTemplate zookeeperContainer) {
        this.zookeeperContainer = zookeeperContainer;
    }

    @Description("Template for the ZooKeeper service account.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getServiceAccount() {
        return serviceAccount;
    }

    public void setServiceAccount(ResourceTemplate serviceAccount) {
        this.serviceAccount = serviceAccount;
    }

    @Description("Template for Secret of the Zookeeper Cluster JMX authentication")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getJmxSecret() {
        return jmxSecret;
    }
    public void setJmxSecret(ResourceTemplate jmxSecret) {
        this.jmxSecret = jmxSecret;
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
