/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation for options to be passed to a JVM.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"-XX", "-Xmx", "-Xms", "gcLoggingEnabled", "javaSystemProperties"})
@EqualsAndHashCode
@ToString
public class JvmOptions implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Configures the default value for the GC logging configuration. This is used in the model classes when the
     * jvmOptions section is not set at all. Storing it here ensures that the default value is the same when jvmOptions
     * is null as well as when jvmOptions are set but without specific gcLoggingEnabled value being set.
     */
    public static final boolean DEFAULT_GC_LOGGING_ENABLED = false;

    private String xmx;
    private String xms;
    private boolean gcLoggingEnabled = DEFAULT_GC_LOGGING_ENABLED;
    private List<SystemProperty> javaSystemProperties;
    private Map<String, String> xx;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @JsonProperty("-Xmx")
    @Pattern("^[0-9]+[mMgG]?$")
    @Description("-Xmx option to to the JVM")
    public String getXmx() {
        return xmx;
    }

    public void setXmx(String xmx) {
        this.xmx = xmx;
    }

    @JsonProperty("-Xms")
    @Pattern("^[0-9]+[mMgG]?$")
    @Description("-Xms option to to the JVM")
    public String getXms() {
        return xms;
    }

    public void setXms(String xms) {
        this.xms = xms;
    }

    @Description("Specifies whether the Garbage Collection logging is enabled. The default is false.")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isGcLoggingEnabled() {
        return gcLoggingEnabled;
    }

    public void setGcLoggingEnabled(boolean gcLoggingEnabled) {
        this.gcLoggingEnabled = gcLoggingEnabled;
    }

    @Description("A map of additional system properties which will be passed using the `-D` option to the JVM.")
    public List<SystemProperty> getJavaSystemProperties() {
        return javaSystemProperties;
    }

    public void setJavaSystemProperties(List<SystemProperty> javaSystemProperties) {
        this.javaSystemProperties = javaSystemProperties;
    }

    @JsonProperty("-XX")
    @Description("A map of -XX options to the JVM")
    public Map<String, String> getXx() {
        return xx;
    }

    public void setXx(Map<String, String> xx) {
        this.xx = xx;
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

