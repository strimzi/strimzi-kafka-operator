/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Toleration;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of a Strimzi-managed Kafka "cluster".
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "replicas", "image", "storage",
        "listeners", "authorization", "config",
        "rack", "brokerRackInitImage",
        "affinity", "tolerations",
        "livenessProbe", "readinessProbe",
        "jvmOptions", "resources",
        "metrics", "logging", "tlsSidecar"})
public class KafkaClusterSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_IMAGE", "strimzi/kafka:latest");
    public static final String DEFAULT_INIT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_INIT_IMAGE", "strimzi/kafka-init:latest");
    public static final String DEFAULT_TLS_SIDECAR_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE", "strimzi/kafka-stunnel:latest");

    public static final String FORBIDDEN_PREFIXES = "listeners, advertised., broker., listener., host.name, port, "
            + "inter.broker.listener.name, sasl., ssl., security., password., principal.builder.class, log.dir, "
            + "zookeeper.connect, zookeeper.set.acl, authorizer., super.user";

    protected Storage storage;

    private Map<String, Object> config = new HashMap<>(0);

    private String brokerRackInitImage;

    private Rack rack;

    private Logging logging;

    private TlsSidecar tlsSidecar;
    private int replicas;
    private String image;
    private Resources resources;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private JvmOptions jvmOptions;
    private Map<String, Object> metrics = new HashMap<>(0);
    private Affinity affinity;
    private List<Toleration> tolerations;
    private KafkaListeners listeners;
    private KafkaAuthorization authorization;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The kafka broker config. Properties with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES)
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

    @Description("TLS sidecar configuration")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public TlsSidecar getTlsSidecar() {
        return tlsSidecar;
    }

    public void setTlsSidecar(TlsSidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
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

    @Description("The docker image for the pods.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Resource constraints (limits and requests).")
    public Resources getResources() {
        return resources;
    }

    public void setResources(Resources resources) {
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
    @Description("The Prometheus JMX Exporter configuration. " +
            "See https://github.com/prometheus/jmx_exporter for details of the structure of this configuration.")
    public Map<String, Object> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, Object> metrics) {
        this.metrics = metrics;
    }

    @Description("Pod affinity rules.")
    @KubeLink(group = "core", version = "v1", kind = "affinity")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Affinity getAffinity() {
        return affinity;
    }

    public void setAffinity(Affinity affinity) {
        this.affinity = affinity;
    }

    @Description("Pod's tolerations.")
    @KubeLink(group = "core", version = "v1", kind = "tolerations")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<Toleration> getTolerations() {
        return tolerations;
    }

    public void setTolerations(List<Toleration> tolerations) {
        this.tolerations = tolerations;
    }

    @Description("Configures listeners of Kafka brokers")
    @JsonProperty(required = true)
    public KafkaListeners getListeners() {
        return listeners;
    }

    public void setListeners(KafkaListeners listeners) {
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

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
