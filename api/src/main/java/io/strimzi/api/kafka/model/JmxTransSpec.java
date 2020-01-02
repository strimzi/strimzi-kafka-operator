/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.template.JmxTransOutputDefinitionTemplate;
import io.strimzi.api.kafka.model.template.JmxTransQueryTemplate;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation for options to be passed into setting up the JmxTrans.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"outputDefinitions", "queries", "resources"})
@EqualsAndHashCode
public class JmxTransSpec implements UnknownPropertyPreserving, Serializable {
    public static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    public static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;

    private static final long serialVersionUID = 1L;
    private List<JmxTransOutputDefinitionTemplate> outputDefinitionTemplates = null;
    private List<JmxTransQueryTemplate> queries = null;

    private ResourceRequirements resources;

    private Map<String, Object> additionalProperties = new HashMap<>(0);


    @JsonProperty(value = "outputDefinitions", required = true)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Description("Defines the output hosts that will be referenced later on. " +
            "For more information, see `proc-jmxtrans-deployment.adoc`")
    public List<JmxTransOutputDefinitionTemplate> getOutputDefinitionTemplates() {
        return outputDefinitionTemplates;
    }

    public void setOutputDefinitionTemplates(List<JmxTransOutputDefinitionTemplate>  outputDefinitionTemplates) {
        this.outputDefinitionTemplates = outputDefinitionTemplates;
    }

    @JsonProperty(required = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("Queries to send to the Kafka brokers to define what data should be read from each broker. " +
            "For more information, see `proc-jmxtrans-deployment.adoc`")
    public List<JmxTransQueryTemplate> getQueries() {
        return queries;
    }

    public void setQueries(List<JmxTransQueryTemplate> queries) {
        this.queries = queries;
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Description("CPU and memory resources to reserve.")
    public ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(ResourceRequirements resources) {
        this.resources = resources;
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