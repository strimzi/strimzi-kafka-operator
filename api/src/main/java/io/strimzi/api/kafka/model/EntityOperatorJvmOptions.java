/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.io.Serializable;

/**
 * Representation for options to be passed to a JVM.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class EntityOperatorJvmOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private boolean gcLoggingDisabled = false;

    @Description("Disable garbage collection logging")
    public boolean isGcLoggingDisabled() {
        return gcLoggingDisabled;
    }

    public void setGcLoggingDisabled(boolean gcLoggingDisabled) {
        this.gcLoggingDisabled = gcLoggingDisabled;
    }
}

