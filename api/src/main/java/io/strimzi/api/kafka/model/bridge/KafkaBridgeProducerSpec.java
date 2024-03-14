/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.bridge;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Map;

@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"config"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaBridgeProducerSpec extends KafkaBridgeClientSpec {
    private static final long serialVersionUID = 1L;

    public static final String FORBIDDEN_PREFIXES = "ssl., bootstrap.servers, sasl., security.";
    public static final String FORBIDDEN_PREFIX_EXCEPTIONS = "ssl.endpoint.identification.algorithm, ssl.cipher.suites, ssl.protocol, ssl.enabled.protocols";

    @Override
    @Description("The Kafka producer configuration used for producer instances created by the bridge. Properties with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES + " (with the exception of: " + FORBIDDEN_PREFIX_EXCEPTIONS + ").")
    public Map<String, Object> getConfig() {
        return config;
    }
}
