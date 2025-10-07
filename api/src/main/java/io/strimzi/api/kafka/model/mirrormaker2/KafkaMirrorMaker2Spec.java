/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker2;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.connect.AbstractKafkaConnectSpec;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.strimzi.crdgenerator.annotations.RequiredInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"version", "replicas", "image", "connectCluster", "clusters", "target", "mirrors", "resources",
    "livenessProbe", "readinessProbe", "jvmOptions", "jmxOptions", "logging", "clientRackInitImage", "rack",
    "metricsConfig", "tracing", "template", "externalConfiguration" })
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@ToString(callSuper = true)
public class KafkaMirrorMaker2Spec extends AbstractKafkaConnectSpec {
    private List<KafkaMirrorMaker2ClusterSpec> clusters;
    private String connectCluster;
    private KafkaMirrorMaker2TargetClusterSpec target;
    private List<KafkaMirrorMaker2MirrorSpec> mirrors;

    @Description("Kafka clusters for mirroring.")
    @PresentInVersions("v1beta2")
    @Deprecated
    @DeprecatedProperty(description = "The `clusters` section is deprecated and will be removed in the `v1` CRD API. " +
            "Please use the `.spec.target` and `.spec.mirrors[].source` sections instead.")
    public List<KafkaMirrorMaker2ClusterSpec> getClusters() {
        return clusters;
    }

    public void setClusters(List<KafkaMirrorMaker2ClusterSpec> clusters) {
        this.clusters = clusters;
    }

    @Description("The cluster alias used for Kafka Connect. " +
            "The value must match the alias of the *target* Kafka cluster as specified in the `spec.clusters` configuration. " +
            "The target Kafka cluster is used by the underlying Kafka Connect framework for its internal topics.")
    @Deprecated
    @DeprecatedProperty(movedToPath = ".spec.target", description = "The `connectCluster` property is deprecated and will be removed in the `v1` CRD API.")
    @PresentInVersions("v1beta2")
    public String getConnectCluster() {
        return connectCluster;
    }

    public void setConnectCluster(String connectCluster) {
        this.connectCluster = connectCluster;
    }

    @Description("The target Apache Kafka cluster. " +
            "The target Kafka cluster is used by the underlying Kafka Connect framework for its internal topics.")
    @RequiredInVersions("v1+")
    public KafkaMirrorMaker2TargetClusterSpec getTarget() {
        return target;
    }

    public void setTarget(KafkaMirrorMaker2TargetClusterSpec target) {
        this.target = target;
    }

    @Description("Configuration of the MirrorMaker 2 connectors.")
    @RequiredInVersions("v1+")
    public List<KafkaMirrorMaker2MirrorSpec> getMirrors() {
        return mirrors;
    }

    public void setMirrors(List<KafkaMirrorMaker2MirrorSpec> mirrors) {
        this.mirrors = mirrors;
    }
}
