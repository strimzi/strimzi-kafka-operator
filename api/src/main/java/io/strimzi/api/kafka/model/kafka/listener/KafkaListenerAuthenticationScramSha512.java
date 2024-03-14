/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Configures a listener to use SASL SCRAM-SHA-512 for authentication.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaListenerAuthenticationScramSha512 extends KafkaListenerAuthentication {

    private static final long serialVersionUID = 1L;

    public static final String SCRAM_SHA_512 = "scram-sha-512";

    @Description("Must be `" + SCRAM_SHA_512 + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return SCRAM_SHA_512;
    }
}
