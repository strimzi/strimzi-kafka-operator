/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.entityoperator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedType;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.HasLivenessProbe;
import io.strimzi.api.kafka.model.common.HasReadinessProbe;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.Sidecar;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a TLS sidecar container configuration
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"image", "resources", "livenessProbe", "readinessProbe", "logLevel"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Deprecated
@DeprecatedType(replacedWithType = void.class)
public class TlsSidecar extends Sidecar implements HasLivenessProbe, HasReadinessProbe {
    private static final long serialVersionUID = 1L;

    private TlsSidecarLogLevel logLevel = TlsSidecarLogLevel.NOTICE;
    private Probe livenessProbe;
    private Probe readinessProbe;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("The log level for the TLS sidecar. " +
            "Default value is `notice`.")
    @JsonProperty(defaultValue = "notice")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
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
