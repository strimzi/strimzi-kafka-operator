/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.exporter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.HasLivenessProbe;
import io.strimzi.api.kafka.model.common.HasReadinessProbe;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the Kafka Exporter deployment.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({
    "image", "groupRegex", "topicRegex", 
    "groupExcludeRegex", "topicExcludeRegex",
    "resources", "logging", "livenessProbe", "readinessProbe",
    "enableSaramaLogging", "showAllOffsets", "template"})
@EqualsAndHashCode
@ToString
public class KafkaExporterSpec implements HasLivenessProbe, HasReadinessProbe, UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private String image;
    private String groupRegex = ".*";
    private String topicRegex = ".*";
    private String topicExcludeRegex;
    private String groupExcludeRegex;

    private ResourceRequirements resources;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private String logging = "info";
    private boolean enableSaramaLogging;
    private boolean showAllOffsets = true;
    private KafkaExporterTemplate template;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The container image used for the Kafka Exporter pods. "
        + "If no image name is explicitly specified, the image name corresponds to the version specified in the Cluster Operator configuration. "
        + "If an image name is not defined in the Cluster Operator configuration, a default value is used.")       
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Description("Regular expression to specify which consumer groups to collect. " +
            "Default value is `.*`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getGroupRegex() {
        return groupRegex;
    }

    public void setGroupRegex(String groupRegex) {
        this.groupRegex = groupRegex;
    }

    @Description("Regular expression to specify which topics to collect. " +
            "Default value is `.*`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getTopicRegex() {
        return topicRegex;
    }

    public void setTopicRegex(String topicRegex) {
        this.topicRegex = topicRegex;
    }

    @Description("Regular expression to specify which consumer groups to exclude.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getGroupExcludeRegex() {
        return groupExcludeRegex;
    }

    public void setGroupExcludeRegex(String groupExcludeRegex) {  
        this.groupExcludeRegex = groupExcludeRegex;
    }

    @Description("Regular expression to specify which topics to exclude.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getTopicExcludeRegex() {
        return topicExcludeRegex;
    }

    public void setTopicExcludeRegex(String topicExcludeRegex) {
        this.topicExcludeRegex = topicExcludeRegex;
    }

    @Description("Enable Sarama logging, a Go client library used by the Kafka Exporter.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean getEnableSaramaLogging() {
        return enableSaramaLogging;
    }

    public void setEnableSaramaLogging(boolean enableSaramaLogging) {
        this.enableSaramaLogging = enableSaramaLogging;
    }

    @Description("Whether show the offset/lag for all consumer group, otherwise, only show connected consumer groups.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean getShowAllOffsets() {
        return showAllOffsets;
    }

    public void setShowAllOffsets(boolean showAllOffsets) {
        this.showAllOffsets = showAllOffsets;
    }

    @Description("Only log messages with the given severity or above. " +
            "Valid levels: [`info`, `debug`, `trace`]. " +
            "Default log level is `info`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getLogging() {
        return logging;
    }

    public void setLogging(String logging) {
        this.logging = logging;
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
    @Description("Pod liveness check.")
    public Probe getLivenessProbe() {
        return livenessProbe;
    }

    public void setLivenessProbe(Probe livenessProbe) {
        this.livenessProbe = livenessProbe;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("Pod readiness check.")
    public Probe getReadinessProbe() {
        return readinessProbe;
    }

    public void setReadinessProbe(Probe readinessProbe) {
        this.readinessProbe = readinessProbe;
    }

    @Description("Customization of deployment templates and pods.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public KafkaExporterTemplate getTemplate() {
        return template;
    }

    public void setTemplate(KafkaExporterTemplate template) {
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
