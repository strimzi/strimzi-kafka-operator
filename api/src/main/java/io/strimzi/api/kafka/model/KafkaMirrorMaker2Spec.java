/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"replicas", "config", "image",
        "livenessProbe", "readinessProbe", "jvmOptions",
        "affinity", "tolerations", "logging", "metrics", "tracing", "template", "clusters", "mirrors"})
@EqualsAndHashCode(doNotUseGetters = true)
public class KafkaMirrorMaker2Spec extends KafkaConnectSpec {

    private static final long serialVersionUID = 1L;
    
    private List<KafkaMirrorMaker2ClusterSpec> clusters;
    private String connectCluster;
    private List<KafkaMirrorMaker2MirrorSpec> mirrors;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Kafka clusters for mirroring.")
    public List<KafkaMirrorMaker2ClusterSpec> getClusters() {
        return clusters;
    }

    public void setClusters(List<KafkaMirrorMaker2ClusterSpec> clusters) {
        this.clusters = clusters;
    }

    @Description("The cluster alias used for Kafka Connect.")
    public String getConnectCluster() {
        return connectCluster;
    }

    public void setConnectCluster(String connectCluster) {
        this.connectCluster = connectCluster;
    }

    @Description("Configuration of the Mirror Maker 2 connectors.")
    public List<KafkaMirrorMaker2MirrorSpec> getMirrors() {
        return mirrors;
    }

    public void setMirrors(List<KafkaMirrorMaker2MirrorSpec> mirrors) {
        this.mirrors = mirrors;
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
