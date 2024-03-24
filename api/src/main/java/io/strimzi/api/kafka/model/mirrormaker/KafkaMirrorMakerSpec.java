/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.HasConfigurableLogging;
import io.strimzi.api.kafka.model.common.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.common.HasLivenessProbe;
import io.strimzi.api.kafka.model.common.HasReadinessProbe;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Logging;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;
import io.strimzi.api.kafka.model.common.tracing.Tracing;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.OneOf;
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
    "version", "replicas", "image", "consumer", "producer", "resources", "whitelist", "include", "jvmOptions",
    "logging", "metricsConfig", "tracing", "template", "livenessProbe", "readinessProbe"})
@OneOf({@OneOf.Alternative(@OneOf.Alternative.Property("include")), @OneOf.Alternative(@OneOf.Alternative.Property("whitelist"))})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaMirrorMakerSpec extends Spec implements HasConfigurableMetrics, HasConfigurableLogging, HasLivenessProbe, HasReadinessProbe {
    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_REPLICAS = 3;

    private int replicas = DEFAULT_REPLICAS;

    private String version;
    private String image;
    private String whitelist;
    private String include;
    private KafkaMirrorMakerConsumerSpec consumer;
    private KafkaMirrorMakerProducerSpec producer;
    private ResourceRequirements resources;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private JvmOptions jvmOptions;
    private Logging logging;
    private MetricsConfig metricsConfig;
    private Tracing tracing;
    private KafkaMirrorMakerTemplate template;

    @Description("The number of pods in the `Deployment`.")
    @Minimum(0)
    @JsonProperty(required = true)
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @Description("The Kafka MirrorMaker version. Defaults to the latest version. " +
            "Consult the documentation to understand the process required to upgrade or downgrade the version.")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Description("The container image used for Kafka MirrorMaker pods. "
        + "If no image name is explicitly specified, it is determined based on the `spec.version` configuration. "
        + "The image names are specifically mapped to corresponding versions in the Cluster Operator configuration.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Description("List of topics which are included for mirroring. This option allows any regular expression using Java-style regular expressions. " +
            "Mirroring two topics named A and B is achieved by using the expression `A|B`. Or, as a special case, you can mirror all topics using the regular expression `*`. " +
            "You can also specify multiple regular expressions separated by commas.")
    @DeprecatedProperty(movedToPath = "spec.include")
    @PresentInVersions("v1alpha1-v1beta2")
    @Deprecated
    public String getWhitelist() {
        return whitelist;
    }

    public void setWhitelist(String whitelist) {
        this.whitelist = whitelist;
    }

    @Description("List of topics which are included for mirroring. This option allows any regular expression using Java-style regular expressions. " +
            "Mirroring two topics named A and B is achieved by using the expression `A|B`. Or, as a special case, you can mirror all topics using the regular expression `*`. " +
            "You can also specify multiple regular expressions separated by commas.")
    public String getInclude() {
        return include;
    }

    public void setInclude(String include) {
        this.include = include;
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
    @Description("The configuration of tracing in Kafka MirrorMaker.")
    public Tracing getTracing() {
        return tracing;
    }

    public void setTracing(Tracing tracing) {
        this.tracing = tracing;
    }

    @Description("Logging configuration for MirrorMaker.")
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
}
