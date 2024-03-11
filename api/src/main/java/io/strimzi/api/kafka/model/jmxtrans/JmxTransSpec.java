/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.jmxtrans;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.annotations.DeprecatedType;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.api.kafka.model.common.template.JmxTransTemplate;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation for options to be passed into setting up the JmxTrans.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({ "image", "outputDefinitions", "logLevel", "kafkaQueries", "resources", "template" })
@EqualsAndHashCode
@ToString
@Deprecated
@DeprecatedType(replacedWithType = void.class)
public class JmxTransSpec implements UnknownPropertyPreserving, Serializable {
    public static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    public static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;

    private static final long serialVersionUID = 1L;
    protected String image;
    private String logLevel;
    private List<JmxTransOutputDefinitionTemplate> outputDefinitions = null;
    private List<JmxTransQueryTemplate> kafkaQueries = null;
    private ResourceRequirements resources;
    private JmxTransTemplate template;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The image to use for the JmxTrans")
    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Description("Sets the logging level of the JmxTrans deployment." +
            "For more information see, https://github.com/jmxtrans/jmxtrans-agent/wiki/Troubleshooting[JmxTrans Logging Level]")
    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    @JsonProperty(required = true)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Description("Defines the output hosts that will be referenced later on. " +
            "For more information on these properties see, xref:type-JmxTransOutputDefinitionTemplate-reference[`JmxTransOutputDefinitionTemplate` schema reference].")
    public List<JmxTransOutputDefinitionTemplate> getOutputDefinitions() {
        return outputDefinitions;
    }

    public void setOutputDefinitions(List<JmxTransOutputDefinitionTemplate> outputDefinitions) {
        this.outputDefinitions = outputDefinitions;
    }

    @JsonProperty(required = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Queries to send to the Kafka brokers to define what data should be read from each broker. " +
            "For more information on these properties see, xref:type-JmxTransQueryTemplate-reference[`JmxTransQueryTemplate` schema reference].")
    public List<JmxTransQueryTemplate> getKafkaQueries() {
        return kafkaQueries;
    }

    public void setKafkaQueries(List<JmxTransQueryTemplate> kafkaQueries) {
        this.kafkaQueries = kafkaQueries;
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

    @Description("Template for JmxTrans resources.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public JmxTransTemplate getTemplate() {
        return template;
    }

    public void setTemplate(JmxTransTemplate template) {
        this.template = template;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}