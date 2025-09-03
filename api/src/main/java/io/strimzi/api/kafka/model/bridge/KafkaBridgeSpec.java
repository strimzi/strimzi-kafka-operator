/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.bridge;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.common.ClientTls;
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
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;
import io.strimzi.api.kafka.model.common.tracing.Tracing;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "replicas", "image", "bootstrapServers", "tls", "authentication", "http", "adminClient", "consumer",
    "producer", "resources", "jvmOptions", "logging", "clientRackInitImage", "rack",
    "enableMetrics", "metricsConfig", "livenessProbe", "readinessProbe", "template", "tracing"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaBridgeSpec extends Spec implements HasConfigurableLogging, HasConfigurableMetrics, HasLivenessProbe, HasReadinessProbe {
    private static final int DEFAULT_REPLICAS = 1;

    private int replicas = DEFAULT_REPLICAS;

    private String image;
    private KafkaBridgeHttpConfig http;
    private String bootstrapServers;
    private ClientTls tls;
    private KafkaClientAuthentication authentication;
    private KafkaBridgeConsumerSpec consumer;
    private KafkaBridgeProducerSpec producer;
    private KafkaBridgeAdminClientSpec adminClient;
    private ResourceRequirements resources;
    private JvmOptions jvmOptions;
    private Logging logging;
    private boolean enableMetrics;
    private MetricsConfig metricsConfig;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private KafkaBridgeTemplate template;
    private Tracing tracing;
    private String clientRackInitImage;
    private Rack rack;

    @Description("The number of pods in the `Deployment`.  " +
            "Defaults to `1`.")
    @Minimum(0)
    @JsonProperty(defaultValue = "1")
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Deprecated
    @DeprecatedProperty(movedToPath = ".spec.metricsConfig",
            description = "The `enableMetrics` configuration is deprecated and will be removed in the future.")
    @PresentInVersions("v1alpha1-v1beta2")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Enable the metrics for the Kafka Bridge. Default is false.")
    public boolean getEnableMetrics() {
        return enableMetrics;
    }

    public void setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
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

    @Description("Logging configuration for Kafka Bridge.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    @Override
    public Logging getLogging() {
        return logging;
    }

    @Override
    public void setLogging(Logging logging) {
        this.logging = logging;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("JVM Options for pods")
    public JvmOptions getJvmOptions() {
        return jvmOptions;
    }

    public void setJvmOptions(JvmOptions jvmOptions) {
        this.jvmOptions = jvmOptions;
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

    @Description("Authentication configuration for connecting to the cluster.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public KafkaClientAuthentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(KafkaClientAuthentication authentication) {
        this.authentication = authentication;
    }

    @Description("TLS configuration for connecting Kafka Bridge to the cluster.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ClientTls getTls() {
        return tls;
    }

    public void setTls(ClientTls tls) {
        this.tls = tls;
    }

    @Description("A list of host:port pairs for establishing the initial connection to the Kafka cluster.")
    @JsonProperty(required = true)
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Description("Kafka AdminClient related configuration.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaBridgeAdminClientSpec getAdminClient() {
        return adminClient;
    }

    public void setAdminClient(KafkaBridgeAdminClientSpec adminClient) {
        this.adminClient = adminClient;
    }

    @Description("Kafka producer related configuration")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaBridgeProducerSpec getProducer() {
        return producer;
    }

    public void setProducer(KafkaBridgeProducerSpec producer) {
        this.producer = producer;
    }

    @Description("Kafka consumer related configuration.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaBridgeConsumerSpec getConsumer() {
        return consumer;
    }

    public void setConsumer(KafkaBridgeConsumerSpec consumer) {
        this.consumer = consumer;
    }

    @Description("The HTTP related configuration.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public KafkaBridgeHttpConfig getHttp() {
        return http;
    }

    public void setHttp(KafkaBridgeHttpConfig http) {
        this.http = http;
    }

    @Description("The container image used for Kafka Bridge pods. "
        + "If no image name is explicitly specified, the image name corresponds to the image specified in the Cluster Operator configuration. "
        + "If an image name is not defined in the Cluster Operator configuration, a default value is used.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Description("Template for Kafka Bridge resources. " +
            "The template allows users to specify how a `Deployment` and `Pod` is generated.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public KafkaBridgeTemplate getTemplate() {
        return template;
    }

    public void setTemplate(KafkaBridgeTemplate template) {
        this.template = template;
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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The configuration of tracing in Kafka Bridge.")
    public Tracing getTracing() {
        return tracing;
    }

    public void setTracing(Tracing tracing) {
        this.tracing = tracing;
    }


    @Description("The image of the init container used for initializing the `client.rack`.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getClientRackInitImage() {
        return clientRackInitImage;
    }

    public void setClientRackInitImage(String brokerRackInitImage) {
        this.clientRackInitImage = brokerRackInitImage;
    }

    @Description("Configuration of the node label which will be used as the client.rack consumer configuration.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Rack getRack() {
        return rack;
    }

    public void setRack(Rack rack) {
        this.rack = rack;
    }
}
