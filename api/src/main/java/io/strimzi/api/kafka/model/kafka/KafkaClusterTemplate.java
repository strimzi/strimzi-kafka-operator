/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

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
    "podDisruptionBudget", "kafkaContainer", "initContainer", "clusterCaCert", "serviceAccount", "jmxSecret",
    "clusterRoleBinding", "podSet"})
@EqualsAndHashCode
@ToString
public class KafkaClusterTemplate implements HasJmxSecretTemplate, Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private StatefulSetTemplate statefulset;
    private ResourceTemplate podSet;
    private PodTemplate pod;
    private InternalServiceTemplate bootstrapService;
    private InternalServiceTemplate brokersService;
    private ResourceTemplate externalBootstrapService;
    private ResourceTemplate perPodService;
    private ResourceTemplate externalBootstrapRoute;
    private ResourceTemplate perPodRoute;
    private ResourceTemplate externalBootstrapIngress;
    private ResourceTemplate perPodIngress;
    private ResourceTemplate persistentVolumeClaim;
    private ResourceTemplate clusterCaCert;
    private PodDisruptionBudgetTemplate podDisruptionBudget;
    private ContainerTemplate kafkaContainer;
    private ContainerTemplate initContainer;
    private ResourceTemplate clusterRoleBinding;
    private ResourceTemplate serviceAccount;
    private ResourceTemplate jmxSecret;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Template for Kafka `StatefulSet`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Deprecated
    @DeprecatedProperty(description = "Support for StatefulSets was removed in Strimzi 0.35.0. This property is ignored.")
    public StatefulSetTemplate getStatefulset() {
        return statefulset;
    }

    public void setStatefulset(StatefulSetTemplate statefulset) {
        this.statefulset = statefulset;
    }

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

    @Description("Template for Kafka bootstrap `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public InternalServiceTemplate getBootstrapService() {
        return bootstrapService;
    }

    public void setBootstrapService(InternalServiceTemplate bootstrapService) {
        this.bootstrapService = bootstrapService;
    }

    @Description("Template for Kafka broker `Service`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public InternalServiceTemplate getBrokersService() {
        return brokersService;
    }

    public void setBrokersService(InternalServiceTemplate brokersService) {
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

    @Description("Template for the Kafka init container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getInitContainer() {
        return initContainer;
    }

    public void setInitContainer(ContainerTemplate initContainer) {
        this.initContainer = initContainer;
    }

    @Description("Template for Secret with Kafka Cluster certificate public key")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getClusterCaCert() {
        return clusterCaCert;
    }

    public void setClusterCaCert(ResourceTemplate clusterCaCert) {
        this.clusterCaCert = clusterCaCert;
    }

    @Description("Template for the Kafka ClusterRoleBinding.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getClusterRoleBinding() {
        return clusterRoleBinding;
    }

    public void setClusterRoleBinding(ResourceTemplate clusterRoleBinding) {
        this.clusterRoleBinding = clusterRoleBinding;
    }

    @Description("Template for the Kafka service account.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getServiceAccount() {
        return serviceAccount;
    }

    public void setServiceAccount(ResourceTemplate serviceAccount) {
        this.serviceAccount = serviceAccount;
    }

    @Description("Template for Secret of the Kafka Cluster JMX authentication")
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
