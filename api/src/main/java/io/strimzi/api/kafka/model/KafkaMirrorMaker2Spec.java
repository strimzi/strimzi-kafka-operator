/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"replicas", "version", "image", "connectCluster", 
        "clusters", "mirrors", "resources", 
        "livenessProbe", "readinessProbe", "jvmOptions",
        "affinity", "tolerations", "logging", "metrics", "tracing", 
        "template", "externalConfiguration"})
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
public class KafkaMirrorMaker2Spec extends AbstractKafkaConnectSpec {

    private static final long serialVersionUID = 1L;
    
    private List<KafkaMirrorMaker2ClusterSpec> clusters;
    private String connectCluster;
    private List<KafkaMirrorMaker2MirrorSpec> mirrors;

    @Description("Kafka clusters for mirroring.")
    public List<KafkaMirrorMaker2ClusterSpec> getClusters() {
        return clusters;
    }

    public void setClusters(List<KafkaMirrorMaker2ClusterSpec> clusters) {
        this.clusters = clusters;
    }

    @Description("The cluster alias used for Kafka Connect. The alias must match a cluster in the list at `spec.clusters`.")
    @JsonProperty(required = true)
    public String getConnectCluster() {
        return connectCluster;
    }

    public void setConnectCluster(String connectCluster) {
        this.connectCluster = connectCluster;
    }

    @Description("Configuration of the MirrorMaker 2.0 connectors.")
    public List<KafkaMirrorMaker2MirrorSpec> getMirrors() {
        return mirrors;
    }

    public void setMirrors(List<KafkaMirrorMaker2MirrorSpec> mirrors) {
        this.mirrors = mirrors;
    }
}
