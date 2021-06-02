/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.Map;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode(callSuper = true)
public class KafkaBridgeAdminClientSpec extends KafkaBridgeClientSpec {
    private static final long serialVersionUID = 1L;

    public static final String FORBIDDEN_PREFIXES = "";
    public static final String FORBIDDEN_PREFIX_EXCEPTIONS = "";

    @Override
    @Description("The Kafka AdminClient configuration used for AdminClient instances created by the bridge.")
    public Map<String, Object> getConfig() {
        return config;
    }
}
