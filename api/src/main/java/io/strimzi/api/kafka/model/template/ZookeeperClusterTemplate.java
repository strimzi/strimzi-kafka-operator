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
 * Representation of a template for ZooKeeper cluster resources.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "statefulset", "pod", "clientService", "nodesService", "persistentVolumeClaim",
        "podDisruptionBudget", "zookeeperContainer", "tlsSidecarContainer"})
@EqualsAndHashCode
public class ZookeeperClusterTemplate implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private StatefulSetTemplate statefulset;
    private PodTemplate pod;
    private ResourceTemplate clientService;
    private ResourceTemplate nodesService;
    private ResourceTemplate persistentVolumeClaim;
    private PodDisruptionBudgetTemplate podDisruptionBudget;
    private ContainerTemplate zookeeperContainer;
    private ContainerTemplate tlsSidecarContainer;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Template for ZooKeeper `StatefulSet`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public StatefulSetTemplate getStatefulset() {
        return statefulset;
    }

    public void setStatefulset(StatefulSetTemplate statefulset) {
        this.statefulset = statefulset;
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
    public ResourceTemplate getClientService() {
        return clientService;
    }

    public void setClientService(ResourceTemplate clientService) {
        this.clientService = clientService;
    }

    @Description("Template for ZooKeeper nodes `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getNodesService() {
        return nodesService;
    }

    public void setNodesService(ResourceTemplate nodesService) {
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

    @DeprecatedProperty
    @Deprecated
    @Description("Template for the Zookeeper server TLS sidecar container. " +
            "The TLS sidecar is not used anymore and this option will be ignored.")
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
