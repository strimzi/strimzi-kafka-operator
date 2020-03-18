/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.vertx.core.cli.annotations.DefaultValue;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a TLS sidecar container configuration
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode(callSuper = true)
public class TlsSidecar extends Sidecar {
    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    public static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;

    private TlsSidecarLogLevel logLevel = TlsSidecarLogLevel.NOTICE;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The log level for the TLS sidecar. " +
            "Default value is `notice`.")
    @DefaultValue("notice")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public TlsSidecarLogLevel getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(TlsSidecarLogLevel logLevel) {
        this.logLevel = logLevel;
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
}
