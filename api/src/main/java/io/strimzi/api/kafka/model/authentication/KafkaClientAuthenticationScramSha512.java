/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.authentication;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

/**
 * Configures the Kafka client authentication using SASL SCRAM_SHA_512 in client based components
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode(callSuper = true)
public class KafkaClientAuthenticationScramSha512 extends KafkaClientAuthenticationScram {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_SCRAM_SHA_512 = "scram-sha-512";

    @Description("Must be `" + TYPE_SCRAM_SHA_512 + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_SCRAM_SHA_512;
    }
}
