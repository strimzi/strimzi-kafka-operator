/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation for options to be passed to a JVM.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class JvmOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private String xmx;
    private String xms;
    private Boolean server;
    private boolean gcLoggingDisabled;
    private Map<String, String> xx;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @JsonProperty("-Xmx")
    @Pattern("[0-9]+[mMgG]?")
    @Description("-Xmx option to to the JVM")
    public String getXmx() {
        return xmx;
    }

    public void setXmx(String xmx) {
        this.xmx = xmx;
    }

    @JsonProperty("-Xms")
    @Pattern("[0-9]+[mMgG]?")
    @Description("-Xms option to to the JVM")
    public String getXms() {
        return xms;
    }

    public void setXms(String xms) {
        this.xms = xms;
    }

    @JsonProperty("-server")
    @Description("-server option to to the JVM")
    public Boolean isServer() {
        return server;
    }

    public void setServer(Boolean server) {
        this.server = server;
    }

    @Description("Disable garbage collection logging")
    public boolean isGcLoggingDisabled() {
        return gcLoggingDisabled;
    }

    public void setGcLoggingDisabled(boolean gcLoggingDisabled) {
        this.gcLoggingDisabled = gcLoggingDisabled;
    }

    @JsonProperty("-XX")
    @Description("A map of -XX options to the JVM")
    public Map<String, String> getXx() {
        return xx;
    }

    public void setXx(Map<String, String> xx) {
        this.xx = xx;
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

