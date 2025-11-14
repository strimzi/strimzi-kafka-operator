/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a probe configuration for sidecar containers
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"execCommand", "httpGetPath", "httpGetPort", "httpGetScheme", "tcpSocketPort", "initialDelaySeconds", "timeoutSeconds", "periodSeconds", "successThreshold", "failureThreshold"})
@EqualsAndHashCode
@ToString
public class SidecarProbe implements UnknownPropertyPreserving {
    private List<String> execCommand;
    private String httpGetPath;
    private String httpGetPort;
    private String httpGetScheme;
    private String tcpSocketPort;
    private int initialDelaySeconds = 15;
    private int timeoutSeconds = 5;
    private int periodSeconds = 10;
    private int successThreshold = 1;
    private int failureThreshold = 3;
    private Map<String, Object> additionalProperties;

    @Description("Command to execute for exec probe")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> getExecCommand() {
        return execCommand;
    }

    public void setExecCommand(List<String> execCommand) {
        this.execCommand = execCommand;
    }

    @Description("Path for HTTP GET probe")
    public String getHttpGetPath() {
        return httpGetPath;
    }

    public void setHttpGetPath(String httpGetPath) {
        this.httpGetPath = httpGetPath;
    }

    @Description("Port for HTTP GET probe (can be port number or named port)")
    public String getHttpGetPort() {
        return httpGetPort;
    }

    public void setHttpGetPort(String httpGetPort) {
        this.httpGetPort = httpGetPort;
    }

    @Description("Scheme for HTTP GET probe (HTTP or HTTPS)")
    public String getHttpGetScheme() {
        return httpGetScheme;
    }

    public void setHttpGetScheme(String httpGetScheme) {
        this.httpGetScheme = httpGetScheme;
    }

    @Description("Port for TCP socket probe (can be port number or named port)")
    public String getTcpSocketPort() {
        return tcpSocketPort;
    }

    public void setTcpSocketPort(String tcpSocketPort) {
        this.tcpSocketPort = tcpSocketPort;
    }

    @Description("Number of seconds after the container has started before liveness or readiness probes are initiated. " +
            "Minimum value is 0.")
    @Minimum(0)
    public int getInitialDelaySeconds() {
        return initialDelaySeconds;
    }

    public void setInitialDelaySeconds(int initialDelaySeconds) {
        this.initialDelaySeconds = initialDelaySeconds;
    }

    @Description("Number of seconds after which the probe times out. " +
            "Minimum value is 1.")
    @Minimum(1)
    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    @Description("How often (in seconds) to perform the probe. " +
            "Minimum value is 1.")
    @Minimum(1)
    public int getPeriodSeconds() {
        return periodSeconds;
    }

    public void setPeriodSeconds(int periodSeconds) {
        this.periodSeconds = periodSeconds;
    }

    @Description("Minimum consecutive successes for the probe to be considered successful after having failed. " +
            "Minimum value is 1.")
    @Minimum(1)
    public int getSuccessThreshold() {
        return successThreshold;
    }

    public void setSuccessThreshold(int successThreshold) {
        this.successThreshold = successThreshold;
    }

    @Description("Minimum consecutive failures for the probe to be considered failed after having succeeded. " +
            "Minimum value is 1.")
    @Minimum(1)
    public int getFailureThreshold() {
        return failureThreshold;
    }

    public void setFailureThreshold(int failureThreshold) {
        this.failureThreshold = failureThreshold;
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
