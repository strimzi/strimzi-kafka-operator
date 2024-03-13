/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect;

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
import io.strimzi.api.kafka.model.common.Rack;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.common.jmx.HasJmxOptions;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxOptions;
import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;
import io.strimzi.api.kafka.model.common.tracing.Tracing;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "replicas", "version", "image", "resources", 
    "livenessProbe", "readinessProbe", "jvmOptions",  "jmxOptions",
    "logging", "clientRackInitImage", "rack", "metricsConfig", "tracing",
    "template", "externalConfiguration" })
@EqualsAndHashCode(doNotUseGetters = true, callSuper = true)
@ToString(callSuper = true)
public abstract class AbstractKafkaConnectSpec extends Spec implements HasConfigurableMetrics, HasConfigurableLogging, HasJmxOptions, HasLivenessProbe, HasReadinessProbe {
    private static final long serialVersionUID = 1L;

    private Logging logging;
    private int replicas = 3;
    private String version;
    private String image;
    private ResourceRequirements resources;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private KafkaJmxOptions jmxOptions;
    private JvmOptions jvmOptions;
    private MetricsConfig metricsConfig;
    private Tracing tracing;
    private KafkaConnectTemplate template;
    private ExternalConfiguration externalConfiguration;
    private String clientRackInitImage;
    private Rack rack;

    @Description("The number of pods in the Kafka Connect group. " +
            "Defaults to `3`.")
    @JsonProperty(defaultValue = "3")
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Description("Logging configuration for Kafka Connect")
    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    @Override
    public Logging getLogging() {
        return logging;
    }

    @Override
    public void setLogging(Logging logging) {
        this.logging = logging;
    }

    @Description("The Kafka Connect version. Defaults to the latest version. " +
            "Consult the user documentation to understand the process required to upgrade or downgrade the version.")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Description("The container image used for Kafka Connect pods. "
        + "If no image name is explicitly specified, it is determined based on the `spec.version` configuration. "
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
    @Description("The maximum limits for CPU and memory resources and the requested initial resources.")
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
    @Description("JMX Options")
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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The configuration of tracing in Kafka Connect.")
    public Tracing getTracing() {
        return tracing;
    }

    public void setTracing(Tracing tracing) {
        this.tracing = tracing;
    }

    @Description("Template for Kafka Connect and Kafka Mirror Maker 2 resources. " +
            "The template allows users to specify how the `Pods`, `Service`, and other services are generated.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public KafkaConnectTemplate getTemplate() {
        return template;
    }

    public void setTemplate(KafkaConnectTemplate template) {
        this.template = template;
    }

    @Description("Pass data from Secrets or ConfigMaps to the Kafka Connect pods and use them to configure connectors.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ExternalConfiguration getExternalConfiguration() {
        return externalConfiguration;
    }

    public void setExternalConfiguration(ExternalConfiguration externalConfiguration) {
        this.externalConfiguration = externalConfiguration;
    }

    @Description("The image of the init container used for initializing the `client.rack`.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getClientRackInitImage() {
        return clientRackInitImage;
    }

    public void setClientRackInitImage(String brokerRackInitImage) {
        this.clientRackInitImage = brokerRackInitImage;
    }

    @Description("Configuration of the node label which will be used as the `client.rack` consumer configuration.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Rack getRack() {
        return rack;
    }

    public void setRack(Rack rack) {
        this.rack = rack;
    }
}
