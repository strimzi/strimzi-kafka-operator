/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

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
public class KafkaClusterTemplate implements Serializable {
    private static final long serialVersionUID = 1L;

    private ResourceTemplate statefulset;
    private ResourceTemplate pod;
    private ResourceTemplate bootstrapService;
    private ResourceTemplate brokersService;
    private ResourceTemplate externalBootstrapService;
    private ResourceTemplate perPodService;
    private ResourceTemplate externalBootstrapRoute;
    private ResourceTemplate perPodRoute;
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
    public ResourceTemplate getPod() {
        return pod;
    }

    public void setPod(ResourceTemplate pod) {
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

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
