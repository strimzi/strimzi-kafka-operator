/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.KafkaClusterTemplate;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.MinimumItems;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of a Strimzi-managed Kafka "cluster".
 */
@DescriptionFile 
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "version", "replicas", "image", "listeners", "config", "storage", "authorization", "rack", "brokerRackInitImage",
    "livenessProbe", "readinessProbe", "jvmOptions", "jmxOptions", "resources", "metricsConfig", "logging", "template"})
@EqualsAndHashCode
public class KafkaClusterSpec implements HasConfigurableMetrics, UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String FORBIDDEN_PREFIXES = "listeners, advertised., broker., listener., host.name, port, "
            + "inter.broker.listener.name, sasl., ssl., security., password., log.dir, "
            + "zookeeper.connect, zookeeper.set.acl, zookeeper.ssl, zookeeper.clientCnxnSocket, authorizer., super.user, "
            + "cruise.control.metrics.topic, cruise.control.metrics.reporter.bootstrap.servers,"
            + "node.id, process.roles, controller."; // KRaft options

    public static final String FORBIDDEN_PREFIX_EXCEPTIONS = "zookeeper.connection.timeout.ms, sasl.server.max.receive.size,"
            + "ssl.cipher.suites, ssl.protocol, ssl.enabled.protocols, ssl.secure.random.implementation,"
            + "cruise.control.metrics.topic.num.partitions, cruise.control.metrics.topic.replication.factor, cruise.control.metrics.topic.retention.ms,"
            + "cruise.control.metrics.topic.auto.create.retries, cruise.control.metrics.topic.auto.create.timeout.ms,"
            + "cruise.control.metrics.topic.min.insync.replicas,"
            + "controller.quorum.election.backoff.max.ms, controller.quorum.election.timeout.ms, controller.quorum.fetch.timeout.ms"; // KRaft options

    protected Storage storage;
    private String version;
    private Map<String, Object> config = new HashMap<>(0);
    private String brokerRackInitImage;
    private Rack rack;
    private Logging logging;
    private int replicas;
    private String image;
    private ResourceRequirements resources;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private JvmOptions jvmOptions;
    private KafkaJmxOptions jmxOptions;
    private MetricsConfig metricsConfig;
    private List<GenericKafkaListener> listeners;
    private KafkaAuthorization authorization;
    private KafkaClusterTemplate template;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The kafka broker version. Defaults to {DefaultKafkaVersion}. " +
            "Consult the user documentation to understand the process required to upgrade or downgrade the version.")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Description("Kafka broker config properties with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES + " (with the exception of: " + FORBIDDEN_PREFIX_EXCEPTIONS + ").")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Description("The image of the init container used for initializing the `broker.rack`.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getBrokerRackInitImage() {
        return brokerRackInitImage;
    }

    public void setBrokerRackInitImage(String brokerRackInitImage) {
        this.brokerRackInitImage = brokerRackInitImage;
    }

    @Description("Configuration of the `broker.rack` broker config.")
    @JsonProperty("rack")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Rack getRack() {
        return rack;
    }

    public void setRack(Rack rack) {
        this.rack = rack;
    }

    @Description("Storage configuration (disk). Cannot be updated.")
    @JsonProperty(required = true)
    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    @Description("Logging configuration for Kafka")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Logging getLogging() {
        return logging;
    }

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

    @Description("The docker image for the pods. The default value depends on the configured `Kafka.spec.kafka.version`.")
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

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
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
    @Description("JMX Options for Kafka brokers")
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

    @Description("Configures listeners of Kafka brokers")
    @MinimumItems(1)
    @JsonProperty(required = true)
    public List<GenericKafkaListener> getListeners() {
        return listeners;
    }

    public void setListeners(List<GenericKafkaListener> listeners) {
        this.listeners = listeners;
    }

    @Description("Authorization configuration for Kafka brokers")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaAuthorization getAuthorization() {
        return authorization;
    }

    public void setAuthorization(KafkaAuthorization authorization) {
        this.authorization = authorization;
    }

    @Description("Template for Kafka cluster resources. " +
            "The template allows users to specify how the `StatefulSet`, `Pods`, and `Services` are generated.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public KafkaClusterTemplate getTemplate() {
        return template;
    }

    public void setTemplate(KafkaClusterTemplate template) {
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
