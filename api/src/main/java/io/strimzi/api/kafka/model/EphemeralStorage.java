/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

/**
 * Representation for ephemeral storage.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EphemeralStorage extends SingleVolumeStorage {

    private static final long serialVersionUID = 1L;

    @Description("Must be `" + TYPE_EPHEMERAL + "`")
    @Override
    public String getType() {
        return TYPE_EPHEMERAL;
    }
}
