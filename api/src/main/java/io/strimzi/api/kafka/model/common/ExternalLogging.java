/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Logging config comes from an existing, user-supplied config map
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonPropertyOrder({"type", "valueFrom"})
@JsonInclude(JsonInclude.Include.NON_NULL)
// TODO remove this Note OneOf constraint declared on superclass
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ExternalLogging extends Logging {

    private static final long serialVersionUID = 1L;
    public static final String TYPE_EXTERNAL = "external";

    private ExternalConfigurationReference valueFrom;

    @Description("Must be `" + TYPE_EXTERNAL + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_EXTERNAL;
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
