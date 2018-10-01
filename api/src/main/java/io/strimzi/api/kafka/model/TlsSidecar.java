/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.vertx.core.cli.annotations.DefaultValue;

/**
 * Representation of a TLS sidecar container configuration
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TlsSidecar extends Sidecar {
    private static final long serialVersionUID = 1L;

    private TlsSidecarLogLevel logLevel = TlsSidecarLogLevel.NOTICE;

    @Description("The log level for the TLS sidecar." +
            "Default value is `notice`.")
    @DefaultValue("notice")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public TlsSidecarLogLevel getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(TlsSidecarLogLevel logLevel) {
        this.logLevel = logLevel;
    }
}
