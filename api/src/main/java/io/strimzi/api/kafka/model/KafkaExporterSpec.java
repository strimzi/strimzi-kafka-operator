/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.template.KafkaExporterTemplate;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the Kafka Exporter deployment.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "replicas", "image", "groupRegex",
        "topicRegex", "watchedNamespace",
        "resources", "logging",
        "enableSarama", "template"})
@EqualsAndHashCode
public class KafkaExporterSpec implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;
    public static final int DEFAULT_REPLICAS = 1;

    private int replicas;
    private String image;
    private String groupRegex;
    private String topicRegex;

    private String watchedNamespace;
    private ResourceRequirements resources;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private String logging;
    private boolean enableSarama;
    private KafkaExporterTemplate template;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The namespace the Exporter Operator should watch.")
    public String getWatchedNamespace() {
        return watchedNamespace;
    }

    public void setWatchedNamespace(String watchedNamespace) {
        this.watchedNamespace = watchedNamespace;
    }

    @Description("The number of pods in the `Deployment`.")
    @Minimum(1)
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Description("The docker image for the pods.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Description("Regex that determines which consumer groups to collect.")
    @JsonProperty(required = true)
    public String getGroupRegex() {
        return groupRegex;
    }

    public void setGroupRegex(String groupRegex) {
        this.groupRegex = groupRegex;
    }

    @Description("Regex that determines which topics to collect.")
    @JsonProperty(required = true)
    public String getTopicRegex() {
        return topicRegex;
    }

    public void setTopicRegex(String topicRegex) {
        this.topicRegex = topicRegex;
    }

    @Description("Turn on Sarama logging.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public boolean getEnableSarama() {
        return enableSarama;
    }

    public void setEnableSarama(boolean enableSarama) {
        this.enableSarama = enableSarama;
    }

    @Description("Only log messages with the given severity or above. Valid levels: [debug, info, warn, error, fatal].")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getLogging() {
        return logging;
    }

    public void setLogging(String logging) {
        this.logging = logging;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Resource constraints (limits and requests).")
    public ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(ResourceRequirements resources) {
        this.resources = resources;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("Pod liveness checking.")
    public Probe getLivenessProbe() {
        return livenessProbe;
    }

    public void setLivenessProbe(Probe livenessProbe) {
        this.livenessProbe = livenessProbe;
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Description("Pod readiness checking.")
    public Probe getReadinessProbe() {
        return readinessProbe;
    }

    public void setReadinessProbe(Probe readinessProbe) {
        this.readinessProbe = readinessProbe;
    }

    @Description("Template for Kafka Exporter resources. " +
            "The template allows users to specify how is the `Deployment` and `Pods` generated.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public KafkaExporterTemplate getTemplate() {
        return template;
    }

    public void setTemplate(KafkaExporterTemplate template) {
        this.template = template;
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
