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
 * Representation of a template for Kafka cluster resources.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "statefulset", "pod", "bootstrapService", "brokersService"})
@EqualsAndHashCode
public class KafkaClusterTemplate implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private ResourceTemplate statefulset;
    private PodTemplate pod;
    private ResourceTemplate bootstrapService;
    private ResourceTemplate brokersService;
    private ResourceTemplate externalBootstrapService;
    private ResourceTemplate perPodService;
    private ResourceTemplate externalBootstrapRoute;
    private ResourceTemplate perPodRoute;
    private ResourceTemplate externalBootstrapIngress;
    private ResourceTemplate perPodIngress;
    private PodDisruptionBudgetTemplate podDisruptionBudget;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Template for Kafka `StatefulSet`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getStatefulset() {
        return statefulset;
    }

    public void setStatefulset(ResourceTemplate statefulset) {
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
    public ResourceTemplate getExternalBootstrapService() {
        return externalBootstrapService;
    }

    public void setExternalBootstrapService(ResourceTemplate externalBootstrapService) {
        this.externalBootstrapService = externalBootstrapService;
    }

    @Description("Template for Kafka per-pod `Services` used for access from outside of Kubernetes.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getPerPodService() {
        return perPodService;
    }

    public void setPerPodService(ResourceTemplate perPodService) {
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

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
