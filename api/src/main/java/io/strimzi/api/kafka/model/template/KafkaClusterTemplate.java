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
 * Representation of a template for Kafka cluster resources.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "statefulset", "pod", "bootstrapService", "brokersService", "externalBootstrapService", "perPodService",
        "externalBootstrapRoute", "perPodRoute", "externalBootstrapIngress", "perPodIngress", "persistentVolumeClaim",
        "podDisruptionBudget", "kafkaContainer", "tlsSidecarContainer", "initContainer"})
@EqualsAndHashCode
public class KafkaClusterTemplate implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private StatefulSetTemplate statefulset;
    private PodTemplate pod;
    private ResourceTemplate bootstrapService;
    private ResourceTemplate brokersService;
    private ExternalServiceTemplate externalBootstrapService;
    private ExternalServiceTemplate perPodService;
    private ResourceTemplate externalBootstrapRoute;
    private ResourceTemplate perPodRoute;
    private ResourceTemplate externalBootstrapIngress;
    private ResourceTemplate perPodIngress;
    private ResourceTemplate persistentVolumeClaim;
    private PodDisruptionBudgetTemplate podDisruptionBudget;
    private ContainerTemplate kafkaContainer;
    private ContainerTemplate tlsSidecarContainer;
    private ContainerTemplate initContainer;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Template for Kafka `StatefulSet`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public StatefulSetTemplate getStatefulset() {
        return statefulset;
    }

    public void setStatefulset(StatefulSetTemplate statefulset) {
        this.statefulset = statefulset;
    }

    @Description("Template for Kafka `Pods`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodTemplate getPod() {
        return pod;
    }

    public void setPod(PodTemplate pod) {
        this.pod = pod;
    }

    @Description("Template for Kafka bootstrap `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getBootstrapService() {
        return bootstrapService;
    }

    public void setBootstrapService(ResourceTemplate bootstrapService) {
        this.bootstrapService = bootstrapService;
    }

    @Description("Template for Kafka broker `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getBrokersService() {
        return brokersService;
    }

    public void setBrokersService(ResourceTemplate brokersService) {
        this.brokersService = brokersService;
    }

    @Description("Template for Kafka external bootstrap `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ExternalServiceTemplate getExternalBootstrapService() {
        return externalBootstrapService;
    }

    public void setExternalBootstrapService(ExternalServiceTemplate externalBootstrapService) {
        this.externalBootstrapService = externalBootstrapService;
    }

    @Description("Template for Kafka per-pod `Services` used for access from outside of Kubernetes.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ExternalServiceTemplate getPerPodService() {
        return perPodService;
    }

    public void setPerPodService(ExternalServiceTemplate perPodService) {
        this.perPodService = perPodService;
    }

    @Description("Template for Kafka external bootstrap `Ingress`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getExternalBootstrapIngress() {
        return externalBootstrapIngress;
    }

    public void setExternalBootstrapIngress(ResourceTemplate externalBootstrapIngress) {
        this.externalBootstrapIngress = externalBootstrapIngress;
    }

    @Description("Template for Kafka per-pod `Ingress` used for access from outside of Kubernetes.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getPerPodIngress() {
        return perPodIngress;
    }

    public void setPerPodIngress(ResourceTemplate perPodIngress) {
        this.perPodIngress = perPodIngress;
    }

    @Description("Template for all Kafka `PersistentVolumeClaims`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getPersistentVolumeClaim() {
        return persistentVolumeClaim;
    }

    public void setPersistentVolumeClaim(ResourceTemplate persistentVolumeClaim) {
        this.persistentVolumeClaim = persistentVolumeClaim;
    }

    @Description("Template for Kafka `PodDisruptionBudget`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodDisruptionBudgetTemplate getPodDisruptionBudget() {
        return podDisruptionBudget;
    }

    public void setPodDisruptionBudget(PodDisruptionBudgetTemplate podDisruptionBudget) {
        this.podDisruptionBudget = podDisruptionBudget;
    }

    @Description("Template for Kafka external bootstrap `Route`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getExternalBootstrapRoute() {
        return externalBootstrapRoute;
    }

    public void setExternalBootstrapRoute(ResourceTemplate externalBootstrapRoute) {
        this.externalBootstrapRoute = externalBootstrapRoute;
    }

    @Description("Template for Kafka per-pod `Routes` used for access from outside of OpenShift.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getPerPodRoute() {
        return perPodRoute;
    }

    public void setPerPodRoute(ResourceTemplate perPodRoute) {
        this.perPodRoute = perPodRoute;
    }

    @Description("Template for the Kafka broker container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getKafkaContainer() {
        return kafkaContainer;
    }

    public void setKafkaContainer(ContainerTemplate kafkaContainer) {
        this.kafkaContainer = kafkaContainer;
    }

    @DeprecatedProperty
    @Deprecated
    @Description("Template for the Kafka broker TLS sidecar container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getTlsSidecarContainer() {
        return tlsSidecarContainer;
    }

    public void setTlsSidecarContainer(ContainerTemplate tlsSidecarContainer) {
        this.tlsSidecarContainer = tlsSidecarContainer;
    }

    @Description("Template for the Kafka init container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getInitContainer() {
        return initContainer;
    }

    public void setInitContainer(ContainerTemplate initContainer) {
        this.initContainer = initContainer;
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
