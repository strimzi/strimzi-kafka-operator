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
@JsonPropertyOrder({"type", "name"})
@JsonInclude(JsonInclude.Include.NON_NULL)
// TODO remove this Note OneOf constraint declared on superclass
public class ExternalLogging extends Logging {

    private static final long serialVersionUID = 1L;
    public static final String TYPE_EXTERNAL = "external";

    private String name;

    private ExternalConfigurationReference valueFrom;

    @Description("Must be `" + TYPE_EXTERNAL + "`")
    @Override
    public String getType() {
        return TYPE_EXTERNAL;
    }

    @Description("The name of the `ConfigMap` from which to get the logging configuration.")
    @DeprecatedProperty(description = "Replaced with valueFrom", removalVersion = "v1beta2")
    @Deprecated
    @PresentInVersions("v1alpha1-v1beta1")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Description("`ConfigMap` entry where the logging configuration is stored. ")
    @PresentInVersions("v1alpha1+")
    public ExternalConfigurationReference getValueFrom() {
        return valueFrom;
    }

    public void setValueFrom(ExternalConfigurationReference valueFrom) {
        this.valueFrom = valueFrom;
    }
}
