/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

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

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "replicas", "image", "whitelist",
        "consumer", "producer", "resources",
        "affinity", "tolerations", "jvmOptions",
        "logging", "metrics"})
public class KafkaMirrorMakerSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_MIRRORMAKER_IMAGE", "strimzi/kafka-mirror-maker:latest");

    private int replicas;
    private String image;
    private String whitelist;
    private KafkaMirrorMakerConsumerSpec consumer;
    private KafkaMirrorMakerProducerSpec producer;
    private Resources resources;
    private Affinity affinity;
    private List<Toleration> tolerations;
    private JvmOptions jvmOptions;
    private Logging logging;
    private Map<String, Object> metrics = new HashMap<>(0);

    @Description("The number of pods in the `Deployment`.")
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

    @Description("List of topics which are included for mirroring. This option allows any regular expression using Java-style regular expressions." +
            "Mirroring two topics named A and B can be achieved by using the whitelist `'A|B'`. Or, as a special case, you can mirror all topics using the whitelist '*'`. " +
            "Multiple regular expressions separated by commas can be specified as well.")
    @JsonProperty(required = true)
    public String getWhitelist() {
        return whitelist;
    }

    public void setWhitelist(String whitelist) {
        this.whitelist = whitelist;
    }

    @Description("Configuration of source cluster.")
    @JsonProperty(required = true)
    public KafkaMirrorMakerConsumerSpec getConsumer() {
        return consumer;
    }

    public void setConsumer(KafkaMirrorMakerConsumerSpec consumer) {
        this.consumer = consumer;
    }

    @Description("Configuration of target cluster.")
    @JsonProperty(required = true)
    public KafkaMirrorMakerProducerSpec getProducer() {
        return producer;
    }

    public void setProducer(KafkaMirrorMakerProducerSpec producer) {
        this.producer = producer;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The Prometheus JMX Exporter configuration. " +
            "See {JMXExporter} for details of the structure of this configuration.")
    public Map<String, Object> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, Object> metrics) {
        this.metrics = metrics;
    }

    @Description("Logging configuration for Mirror Maker.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Logging getLogging() {
        return logging;
    }

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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Resource constraints (limits and requests).")
    public Resources getResources() {
        return resources;
    }

    public void setResources(Resources resources) {
        this.resources = resources;
    }
}
