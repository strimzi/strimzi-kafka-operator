/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.zookeeper;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.HasConfigurableLogging;
import io.strimzi.api.kafka.model.common.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.common.HasLivenessProbe;
import io.strimzi.api.kafka.model.common.HasReadinessProbe;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Logging;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.api.kafka.model.common.jmx.HasJmxOptions;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxOptions;
import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a Strimzi-managed ZooKeeper "cluster".
 */
@DescriptionFile 
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "replicas", "image", "storage", "config", "livenessProbe", "readinessProbe", "jvmOptions", "jmxOptions", "resources",
    "metricsConfig", "logging", "template"})
@EqualsAndHashCode
@ToString
public class ZookeeperClusterSpec implements HasConfigurableMetrics, HasConfigurableLogging, HasJmxOptions, HasLivenessProbe, HasReadinessProbe, UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String FORBIDDEN_PREFIXES = "server., dataDir, dataLogDir, clientPort, authProvider, " +
            "quorum.auth, requireClientAuthScheme, snapshot.trust.empty, standaloneEnabled, " +
            "reconfigEnabled, 4lw.commands.whitelist, secureClientPort, ssl., serverCnxnFactory, sslQuorum";
    public static final String FORBIDDEN_PREFIX_EXCEPTIONS = "ssl.protocol, ssl.quorum.protocol, ssl.enabledProtocols, " +
            "ssl.quorum.enabledProtocols, ssl.ciphersuites, ssl.quorum.ciphersuites, ssl.hostnameVerification, ssl.quorum.hostnameVerification";

    protected SingleVolumeStorage storage;
    private Map<String, Object> config = new HashMap<>(0);
    private Logging logging;
    private int replicas;
    private String image;
    private ResourceRequirements resources;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private JvmOptions jvmOptions;
    private KafkaJmxOptions jmxOptions;
    private MetricsConfig metricsConfig;
    private ZookeeperClusterTemplate template;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The ZooKeeper broker config. Properties with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES + " (with the exception of: " + FORBIDDEN_PREFIX_EXCEPTIONS + ").")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Description("Storage configuration (disk). Cannot be updated.")
    @JsonProperty(required = true)
    public SingleVolumeStorage getStorage() {
        return storage;
    }

    public void setStorage(SingleVolumeStorage storage) {
        this.storage = storage;
    }

    @Description("Logging configuration for ZooKeeper")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Override
    public Logging getLogging() {
        return logging;
    }

    @Override
    public void setLogging(Logging logging) {
        this.logging = logging;
    }

    @Description("The number of pods in the cluster.")
    @Minimum(1)
    @JsonProperty(required = true)
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Description("The container image used for ZooKeeper pods. "
        + "If no image name is explicitly specified, it is determined based on the Kafka version set in `spec.kafka.version`. "
        + "The image names are specifically mapped to corresponding versions in the Cluster Operator configuration.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @KubeLink(group = "core", version = "v1", kind = "resourcerequirements")
    @Description("CPU and memory resources to reserve.")
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

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("Pod readiness checking.")
    public Probe getReadinessProbe() {
        return readinessProbe;
    }

    public void setReadinessProbe(Probe readinessProbe) {
        this.readinessProbe = readinessProbe;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("JVM Options for pods")
    public JvmOptions getJvmOptions() {
        return jvmOptions;
    }

    public void setJvmOptions(JvmOptions jvmOptions) {
        this.jvmOptions = jvmOptions;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("JMX Options for Zookeeper nodes")
    @JsonProperty("jmxOptions")
    public KafkaJmxOptions getJmxOptions() {
        return jmxOptions;
    }

    public void setJmxOptions(KafkaJmxOptions jmxOptions) {
        this.jmxOptions = jmxOptions;
    }

    @Description("Metrics configuration.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Override
    public MetricsConfig getMetricsConfig() {
        return metricsConfig;
    }

    @Override
    public void setMetricsConfig(MetricsConfig metricsConfig) {
        this.metricsConfig = metricsConfig;
    }

    @Description("Template for ZooKeeper cluster resources. " +
            "The template allows users to specify how the Kubernetes resources are generated.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ZookeeperClusterTemplate getTemplate() {
        return template;
    }

    public void setTemplate(ZookeeperClusterTemplate template) {
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
