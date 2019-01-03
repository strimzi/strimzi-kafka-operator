/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.template.ClusterOperatorTemplate;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;

/**
 * Representation of a Strimzi-managed Cluster Operator.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "replicas", "template", "strategy"})
public class ClusterOperatorSpec implements Serializable {
    private static final long serialVersionUID = 1L;

    private int replicas;
    private ClusterOperatorTemplate template;
    private Strategy strategy;

    @Description("The number of pods in the cluster.")
    @Minimum(1)
    @JsonProperty(required = true)
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Description("Template for Cluster Operator resources.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ClusterOperatorTemplate getTemplate() {
        return template;
    }

    public void setTemplate(ClusterOperatorTemplate template) {
        this.template = template;
    }

    @Description("Storage configuration (disk). Cannot be updated.")
    @JsonProperty(required = true)
    public Strategy getStrategy() {
        return strategy;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }
}
