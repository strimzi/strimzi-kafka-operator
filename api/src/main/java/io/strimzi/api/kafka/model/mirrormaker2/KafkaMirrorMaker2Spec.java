/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker2;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.connect.AbstractKafkaConnectSpec;
import io.strimzi.crdgenerator.annotations.Description;
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
@JsonPropertyOrder({"version", "replicas", "image", "target", "mirrors", "resources", "livenessProbe", "readinessProbe",
    "jvmOptions", "jmxOptions", "logging", "clientRackInitImage", "rack", "metricsConfig", "tracing", "template" })
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@ToString(callSuper = true)
public class KafkaMirrorMaker2Spec extends AbstractKafkaConnectSpec {
    private KafkaMirrorMaker2TargetClusterSpec target;
    private List<KafkaMirrorMaker2MirrorSpec> mirrors;

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
