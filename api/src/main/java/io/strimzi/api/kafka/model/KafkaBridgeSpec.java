/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.template.KafkaBridgeTemplate;
import io.strimzi.api.kafka.model.tracing.Tracing;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "replicas", "image", "bootstrapServers", "tls", "authentication", "http", "adminClient", "consumer",
    "producer", "resources", "jvmOptions", "logging", "clientRackInitImage", "rack",
    "enableMetrics", "livenessProbe", "readinessProbe", "template", "tracing"})
@EqualsAndHashCode
public class KafkaBridgeSpec extends Spec {
    private static final long serialVersionUID = 1L;
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
    private Probe livenessProbe;
    private Probe readinessProbe;
    private KafkaBridgeTemplate template;
    private Tracing tracing;
    private String clientRackInitImage;
    private Rack rack;

    @Description("The number of pods in the `Deployment`.")
    @Minimum(0)
    @JsonProperty(defaultValue = "1")
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Enable the metrics for the Kafka Bridge. Default is false.")
    public boolean getEnableMetrics() {
        return enableMetrics;
    }

    public void setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
    }

    @Description("Logging configuration for Kafka Bridge.")
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

    @Description("The docker image for the pods.")
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
