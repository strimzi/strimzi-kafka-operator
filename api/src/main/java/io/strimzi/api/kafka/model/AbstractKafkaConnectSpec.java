/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Toleration;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.connect.ExternalConfiguration;
import io.strimzi.api.kafka.model.template.KafkaConnectTemplate;
import io.strimzi.api.kafka.model.tracing.Tracing;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.sundr.builder.annotations.Buildable;
import io.vertx.core.cli.annotations.DefaultValue;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "replicas", "version", "image", "resources", 
        "livenessProbe", "readinessProbe", "jvmOptions",  "jmxOptions",
        "affinity", "tolerations", "logging", "metrics", "metricsConfig", "tracing",
        "template", "externalConfiguration"})
@EqualsAndHashCode(doNotUseGetters = true)
public abstract class AbstractKafkaConnectSpec extends Spec implements HasConfigurableMetrics {
    private static final long serialVersionUID = 1L;

    private Logging logging;
    private Integer replicas;

    private String version;
    private String image;
    private ResourceRequirements resources;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private KafkaJmxOptions jmxOptions;
    private JvmOptions jvmOptions;
    private MetricsConfig metricsConfig;
    private Map<String, Object> metrics;
    private Tracing tracing;
    private Affinity affinity;
    private List<Toleration> tolerations;
    private KafkaConnectTemplate template;
    private ExternalConfiguration externalConfiguration;

    @Description("The number of pods in the Kafka Connect group.")
    @DefaultValue("3")
    public Integer getReplicas() {
        return replicas;
    }

    @Description("Logging configuration for Kafka Connect")
    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    public Logging getLogging() {
        return logging;
    }

    public void setLogging(Logging logging) {
        this.logging = logging;
    }

    public void setReplicas(Integer replicas) {
        this.replicas = replicas;
    }

    @Description("The Kafka Connect version. Defaults to {DefaultKafkaVersion}. " +
            "Consult the user documentation to understand the process required to upgrade or downgrade the version.")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Description("The docker image for the pods.")
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

    @DeprecatedProperty(movedToPath = "spec.metricsConfig", removalVersion = "v1beta2")
    @PresentInVersions("v1alpha1-v1beta1")
    @Deprecated
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The Prometheus JMX Exporter configuration. " +
            "See https://github.com/prometheus/jmx_exporter for details of the structure of this configuration.")
    @Override
    public Map<String, Object> getMetrics() {
        return metrics;
    }

    @Override
    public void setMetrics(Map<String, Object> metrics) {
        this.metrics = metrics;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The configuration of tracing in Kafka Connect.")
    public Tracing getTracing() {
        return tracing;
    }

    public void setTracing(Tracing tracing) {
        this.tracing = tracing;
    }

    @PresentInVersions("v1alpha1-v1beta1")
    @Description("The pod's affinity rules.")
    @KubeLink(group = "core", version = "v1", kind = "affinity")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @DeprecatedProperty(movedToPath = "spec.template.pod.affinity", removalVersion = "v1beta2")
    @Deprecated
    public Affinity getAffinity() {
        return affinity;
    }

    @Deprecated
    public void setAffinity(Affinity affinity) {
        this.affinity = affinity;
    }

    @PresentInVersions("v1alpha1-v1beta1")
    @Description("The pod's tolerations.")
    @KubeLink(group = "core", version = "v1", kind = "toleration")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @DeprecatedProperty(movedToPath = "spec.template.pod.tolerations", removalVersion = "v1beta2")
    @Deprecated
    public List<Toleration> getTolerations() {
        return tolerations;
    }

    @Deprecated
    public void setTolerations(List<Toleration> tolerations) {
        this.tolerations = tolerations;
    }

    @Description("Template for Kafka Connect and Kafka Connect S2I resources. " +
            "The template allows users to specify how the `Deployment`, `Pods` and `Service` are generated.")
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
}
