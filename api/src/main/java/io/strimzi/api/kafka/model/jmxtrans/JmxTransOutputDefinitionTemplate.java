/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.jmxtrans;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation for options to define where and how information will be pushed to remote sources of information
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"outputType", "host", "port", "flushDelayInSeconds", "typeNames", "name"})
@EqualsAndHashCode
@ToString
public class JmxTransOutputDefinitionTemplate implements Serializable, UnknownPropertyPreserving {

    private static final long serialVersionUID = 1L;

    private String outputType;
    private String host;
    private Integer port;
    private Integer flushDelayInSeconds;
    private String name;
    private List<String> typeNames;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @JsonProperty(value = "outputType", required = true)
    @Description("Template for setting the format of the data that will be pushed." +
            "For more information see https://github.com/jmxtrans/jmxtrans/wiki/OutputWriters[JmxTrans OutputWriters]")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getOutputType() {
        return outputType;
    }

    public void setOutputType(String outputType) {
        this.outputType = outputType;
    }

    @Description("The DNS/hostname of the remote host that the data is pushed to.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Description("The port of the remote host that the data is pushed to.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    @Description("How many seconds the JmxTrans waits before pushing a new set of data out.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getFlushDelayInSeconds() {
        return flushDelayInSeconds;
    }

    public void setFlushDelayInSeconds(Integer flushDelayInSeconds) {
        this.flushDelayInSeconds = flushDelayInSeconds;
    }

    @Description("Template for filtering data to be included in response to a wildcard query. " +
            "For more information see https://github.com/jmxtrans/jmxtrans/wiki/Queries[JmxTrans queries]")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<String> getTypeNames() {
        return typeNames;
    }

    public void setTypeNames(List<String> typeNames) {
        this.typeNames = typeNames;
    }

    @Description("Template for setting the name of the output definition. This is used to identify where to send " +
            "the results of queries should be sent.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(required = true)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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