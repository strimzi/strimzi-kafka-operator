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
import io.strimzi.api.kafka.model.template.KafkaMirrorMakerTemplate;
import io.strimzi.api.kafka.model.tracing.Tracing;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "replicas", "image", "whitelist",
        "consumer", "producer", "resources",
        "affinity", "tolerations", "jvmOptions",
        "logging", "metrics", "tracing", "template"})
@EqualsAndHashCode
public class KafkaMirrorMakerSpec implements UnknownPropertyPreserving, Serializable {

    private static final long serialVersionUID = 1L;

    private int replicas;
    private String version;
    private String image;
    private String whitelist;
    private KafkaMirrorMakerConsumerSpec consumer;
    private KafkaMirrorMakerProducerSpec producer;
    private ResourceRequirements resources;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private Affinity affinity;
    private List<Toleration> tolerations;
    private JvmOptions jvmOptions;
    private Logging logging;
    private Map<String, Object> metrics;
    private Tracing tracing;
    private KafkaMirrorMakerTemplate template;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The number of pods in the `Deployment`.")
    @Minimum(1)
    @JsonProperty(required = true)
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Description("The Kafka MirrorMaker version. Defaults to {DefaultKafkaVersion}. " +
            "Consult the documentation to understand the process required to upgrade or downgrade the version.")
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

    @Description("List of topics which are included for mirroring. This option allows any regular expression using Java-style regular expressions. " +
            "Mirroring two topics named A and B is achieved by using the whitelist `'A|B'`. Or, as a special case, you can mirror all topics using the whitelist '*'. " +
            "You can also specify multiple regular expressions separated by commas.")
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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The configuration of tracing in Kafka MirrorMaker.")
    public Tracing getTracing() {
        return tracing;
    }

    public void setTracing(Tracing tracing) {
        this.tracing = tracing;
    }

    @Description("Logging configuration for MirrorMaker.")
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

    @Description("The pod's affinity rules.")
    @KubeLink(group = "core", version = "v1", kind = "affinity")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @DeprecatedProperty(movedToPath = "spec.template.pod.affinity")
    @Deprecated
    public Affinity getAffinity() {
        return affinity;
    }

    @Deprecated
    public void setAffinity(Affinity affinity) {
        this.affinity = affinity;
    }

    @Description("The pod's tolerations.")
    @KubeLink(group = "core", version = "v1", kind = "toleration")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @DeprecatedProperty(movedToPath = "spec.template.pod.tolerations")
    @Deprecated
    public List<Toleration> getTolerations() {
        return tolerations;
    }

    @Deprecated
    public void setTolerations(List<Toleration> tolerations) {
        this.tolerations = tolerations;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
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

    @Description("Template to specify how Kafka MirrorMaker resources, `Deployments` and `Pods`, are generated.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public KafkaMirrorMakerTemplate getTemplate() {
        return template;
    }

    public void setTemplate(KafkaMirrorMakerTemplate template) {
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
