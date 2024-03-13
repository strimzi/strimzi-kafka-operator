/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.nodepool;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the template section of a KafkaNodePool
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"podSet", "pod", "perPodService", "perPodRoute", "perPodIngress", "persistentVolumeClaim",
                    "kafkaContainer", "initContainer"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaNodePoolTemplate extends Spec {
    private static final long serialVersionUID = 1L;

    private ResourceTemplate podSet;
    private PodTemplate pod;
    private ResourceTemplate perPodService;
    private ResourceTemplate perPodRoute;
    private ResourceTemplate perPodIngress;
    private ResourceTemplate persistentVolumeClaim;
    private ContainerTemplate kafkaContainer;
    private ContainerTemplate initContainer;
    private final Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Template for Kafka `StrimziPodSet` resource.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getPodSet() {
        return podSet;
    }

    public void setPodSet(ResourceTemplate podSetTemplate) {
        this.podSet = podSetTemplate;
    }

    @Description("Template for Kafka `Pods`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodTemplate getPod() {
        return pod;
    }

    public void setPod(PodTemplate pod) {
        this.pod = pod;
    }

    @Description("Template for Kafka per-pod `Services` used for access from outside of Kubernetes.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getPerPodService() {
        return perPodService;
    }

    public void setPerPodService(ResourceTemplate perPodService) {
        this.perPodService = perPodService;
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
