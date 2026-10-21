/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.sova;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.HasConfigurableLogging;
import io.strimzi.api.kafka.model.common.HasLivenessProbe;
import io.strimzi.api.kafka.model.common.HasReadinessProbe;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.Logging;
import io.strimzi.api.kafka.model.common.StrimziProbe;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of the Sova deployment.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({
    "image", "groupRegex", "groupExcludeRegex", "topicRegex", "topicExcludeRegex",
    "logging", "jvmOptions", "resources", "livenessProbe", "readinessProbe", "template"})
@EqualsAndHashCode
@ToString
public class SovaSpec implements HasConfigurableLogging, HasLivenessProbe, HasReadinessProbe, UnknownPropertyPreserving {
    private String image;
    private String groupRegex = ".*";
    private String groupExcludeRegex;
    private String topicRegex = ".*";
    private String topicExcludeRegex;
    private Logging logging;
    private JvmOptions jvmOptions;
    private ResourceRequirements resources;
    private StrimziProbe livenessProbe;
    private StrimziProbe readinessProbe;
    private SovaTemplate template;

    private Map<String, Object> additionalProperties;

    @Description("The container image used for the Sova pods. "
        + "If no image name is explicitly specified, the image name corresponds to the name specified in the Cluster Operator configuration. "
        + "If an image name is not defined in the Cluster Operator configuration, a default value is used.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Description("Regular expression to specify which consumer groups to collect. "
        + "Default value is `.*`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getGroupRegex() {
        return groupRegex;
    }

    public void setGroupRegex(String groupRegex) {
        this.groupRegex = groupRegex;
    }

    @Description("Regular expression to specify which consumer groups to exclude.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getGroupExcludeRegex() {
        return groupExcludeRegex;
    }

    public void setGroupExcludeRegex(String groupExcludeRegex) {
        this.groupExcludeRegex = groupExcludeRegex;
    }

    @Description("Regular expression to specify which topics to collect. "
        + "Default value is `.*`.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getTopicRegex() {
        return topicRegex;
    }

    public void setTopicRegex(String topicRegex) {
        this.topicRegex = topicRegex;
    }

    @Description("Regular expression to specify which topics to exclude.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public String getTopicExcludeRegex() {
        return topicExcludeRegex;
    }

    public void setTopicExcludeRegex(String topicExcludeRegex) {
        this.topicExcludeRegex = topicExcludeRegex;
    }

    @Description("Logging configuration (Log4j 2) for Sova.")
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
    @Description("JVM Options for the Sova container.")
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
    @Description("Pod liveness check.")
    public StrimziProbe getLivenessProbe() {
        return livenessProbe;
    }

    public void setLivenessProbe(StrimziProbe livenessProbe) {
        this.livenessProbe = livenessProbe;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("Pod readiness check.")
    public StrimziProbe getReadinessProbe() {
        return readinessProbe;
    }

    public void setReadinessProbe(StrimziProbe readinessProbe) {
        this.readinessProbe = readinessProbe;
    }

    @Description("Customization of deployment templates and pods.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public SovaTemplate getTemplate() {
        return template;
    }

    public void setTemplate(SovaTemplate template) {
        this.template = template;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
