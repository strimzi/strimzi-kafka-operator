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
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a template for Kafka Exporter resources.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"deployment", "pod", "service", "container", "serviceAccount"})
@EqualsAndHashCode
public class KafkaExporterTemplate implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private DeploymentTemplate deployment;
    private PodTemplate pod;
    private ResourceTemplate service;
    private ContainerTemplate container;
    private ResourceTemplate serviceAccount;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Template for Kafka Exporter `Deployment`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public DeploymentTemplate getDeployment() {
        return deployment;
    }

    public void setDeployment(DeploymentTemplate deployment) {
        this.deployment = deployment;
    }

    @Description("Template for Kafka Exporter `Pods`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodTemplate getPod() {
        return pod;
    }

    public void setPod(PodTemplate pod) {
        this.pod = pod;
    }

    @Description("Template for Kafka Exporter `Service`.")
    @DeprecatedProperty(description = "The Kafka Exporter service has been removed.")
    @PresentInVersions("v1alpha1-v1beta2")
    @Deprecated
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ResourceTemplate getService() {
        return service;
    }

    public void setService(ResourceTemplate service) {
        this.service = service;
    }

    @Description("Template for the Kafka Exporter container")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ContainerTemplate getContainer() {
        return container;
    }

    public void setContainer(ContainerTemplate container) {
        this.container = container;
    }

    @Description("Template for the Kafka Exporter service account.")
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
