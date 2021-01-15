/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.sundr.builder.annotations.Buildable;

/**
 * Logging config comes from an existing, user-supplied config map
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonPropertyOrder({"type", "name", "valueFrom"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExternalLogging extends Logging {

    private static final long serialVersionUID = 1L;
    public static final String TYPE_EXTERNAL = "external";

    private String name;
    private ExternalConfigurationLogging valueFrom;

    @Description("Must be `" + TYPE_EXTERNAL + "`")
    @Override
    public String getType() {
        return TYPE_EXTERNAL;
    }

    @PresentInVersions("v1alpha1-v1beta1")
    @Deprecated
    @DeprecatedProperty(movedToPath = "logging.valueFrom.configMapKeyRef.name")
    @Description("The name is the `ConfigMap` from which to get the logging configuration.")
    public String getName() {
        return name;
    }

    @Description("ConfigMap where the log4j/log4j2 configuration is stored.")
    public ExternalConfigurationLogging getValueFrom() {
        return valueFrom;
    }

    public void setValueFrom(ExternalConfigurationLogging valueFrom) {
        this.valueFrom = valueFrom;
    }

    public void setName(String name) {
        this.name = name;
    }
}
