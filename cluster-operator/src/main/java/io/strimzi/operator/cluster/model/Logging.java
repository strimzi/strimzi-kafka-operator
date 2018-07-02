/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.fabric8.kubernetes.api.model.ConfigMap;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = "inline", value = InlineLogging.class),
        @JsonSubTypes.Type(name = "external", value = ExternalLogging.class),
})
public abstract class Logging {
    Logging() {
        this.cm = null;
    }
    public abstract String getType();

    public ConfigMap getCm() {
        return cm;
    }

    public void setCm(ConfigMap cm) {
        this.cm = cm;
    }

    public ConfigMap cm;
}

