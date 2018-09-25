/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.util.Map;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "bootstrapServers", "logging"})
public class KafkaMirrorMakerProducerSpec extends KafkaMirrorMakerClientSpec {
    private static final long serialVersionUID = 1L;

    public static final String FORBIDDEN_PREFIXES = "ssl., bootstrap.servers, sasl., security.";

    @Override
    @Description("The mirror maker producer config. Properties with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getConfig() {
        return config;
    }

}
