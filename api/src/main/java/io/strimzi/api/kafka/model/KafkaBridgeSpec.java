/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.template.KafkaBridgeTemplate;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "replicas", "bootstrapServers", "authentication",
        "tls", "http", "image", "resources", "jvmOptions",
        "logging", "metrics", "livenessProbe", "readinessProbe", "template"})
@EqualsAndHashCode
public class KafkaBridgeSpec implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private int replicas;
    private String bootstrapServers;

    private KafkaBridgeAuthentication authentication;
    private KafkaBridgeTls tls;
    private KafkaBridgeHttpConfig http;
    private String image;
    private ResourceRequirements resources;
    private JvmOptions jvmOptions;
    private Logging logging;
    private Map<String, Object> metrics;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private Map<String, Object> additionalProperties = new HashMap<>(0);
    private KafkaBridgeTemplate template;

    @Description("The number of pods in the `Deployment`.")
    @Minimum(0)
    @JsonProperty(required = true)
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Description("A list of host:port pairs to use for establishing the connection to the Kafka cluster.")
    @JsonProperty(required = true)
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("**Currently not supported** The Prometheus JMX Exporter configuration. " +
            "See {JMXExporter} for details of the structure of this configuration.")
    public Map<String, Object> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, Object> metrics) {
        this.metrics = metrics;
    }

    @Description("**Currently not supported** Logging configuration for Mirror Maker.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Logging getLogging() {
        return logging;
    }

    public void setLogging(Logging logging) {
        this.logging = logging;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("**Currently not supported** JVM Options for pods")
    public JvmOptions getJvmOptions() {
        return jvmOptions;
    }

    public void setJvmOptions(JvmOptions jvmOptions) {
        this.jvmOptions = jvmOptions;
    }


    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("**Currently not supported** Resource constraints (limits and requests).")
    public ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(ResourceRequirements resources) {
        this.resources = resources;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Description("**Currently not supported** Configures Kafka Bridge authentication.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaBridgeAuthentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(KafkaBridgeAuthentication logging) {
        this.authentication = logging;
    }

    @Description("**Currently not supported** TLS configuration for connecting to the cluster.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaBridgeTls getTls() {
        return tls;
    }

    public void setTls(KafkaBridgeTls kafkaBridgeTls) {
        this.tls = kafkaBridgeTls;
    }

    @Description("The HTTP related configuration.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaBridgeHttpConfig getHttp() {
        return http;
    }

    public void setHttp(KafkaBridgeHttpConfig http) {
        this.http = http;
    }

    @Description("The docker image for the pods.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Description("Template for Kafka Bridge resources. " +
            "The template allows users to specify how is the `Deployment` and `Pods` generated.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public KafkaBridgeTemplate getTemplate() {
        return template;
    }

    public void setTemplate(KafkaBridgeTemplate template) {
        this.template = template;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("**Currently not supported** Pod liveness checking.")
    public Probe getLivenessProbe() {
        return livenessProbe;
    }

    public void setLivenessProbe(Probe livenessProbe) {
        this.livenessProbe = livenessProbe;
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Description("**Currently not supported** Pod readiness checking.")
    public Probe getReadinessProbe() {
        return readinessProbe;
    }

    public void setReadinessProbe(Probe readinessProbe) {
        this.readinessProbe = readinessProbe;
    }
}
