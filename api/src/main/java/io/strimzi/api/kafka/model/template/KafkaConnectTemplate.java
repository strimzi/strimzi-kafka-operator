/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;

/**
 * Representation of a template for Kafka Connect and Kafka Connect S2I resources.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "deployment", "pods", "service"})
public class KafkaConnectTemplate implements Serializable {
    private static final long serialVersionUID = 1L;

    private DeploymentTemplate deployment;
    private PodTemplate pods;
    private ServiceTemplate service;

    @Description("Template for Kafka Connect deployment.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public DeploymentTemplate getDeployment() {
        return deployment;
    }

    public void setDeployment(DeploymentTemplate deployment) {
        this.deployment = deployment;
    }

    @Description("Template for Kafka Connect pods.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public PodTemplate getPods() {
        return pods;
    }

    public void setPods(PodTemplate pods) {
        this.pods = pods;
    }

    @Description("Template for Kafka Connect service.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ServiceTemplate getService() {
        return service;
    }

    public void setService(ServiceTemplate service) {
        this.service = service;
    }
}
