/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker2;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.connect.AbstractKafkaConnectSpec;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"version", "replicas", "image", "connectCluster",
    "clusters", "mirrors", "resources", "livenessProbe", "readinessProbe",
    "jvmOptions", "jmxOptions", "affinity", "tolerations", "logging",
    "clientRackInitImage", "rack", "metricsConfig", "tracing",
    "template", "externalConfiguration" })
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@ToString(callSuper = true)
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

    @Description("The cluster alias used for Kafka Connect. " +
            "The value must match the alias of the *target* Kafka cluster as specified in the `spec.clusters` configuration. " +
            "The target Kafka cluster is used by the underlying Kafka Connect framework for its internal topics.")
    @JsonProperty(required = true)
    public String getConnectCluster() {
        return connectCluster;
    }

    public void setConnectCluster(String connectCluster) {
        this.connectCluster = connectCluster;
    }

    @Description("Configuration of the MirrorMaker 2 connectors.")
    public List<KafkaMirrorMaker2MirrorSpec> getMirrors() {
        return mirrors;
    }

    public void setMirrors(List<KafkaMirrorMaker2MirrorSpec> mirrors) {
        this.mirrors = mirrors;
    }
}
