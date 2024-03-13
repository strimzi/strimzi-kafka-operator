/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.authentication;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.common.PasswordSecretSource;
import io.strimzi.crdgenerator.annotations.Description;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Configures the Kafka client authentication using one of the possible SASL SCRAM_SHA_* methods in client based
 * components
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public abstract class KafkaClientAuthenticationScram extends KafkaClientAuthentication {
    private static final long serialVersionUID = 1L;

    @Description("Reference to the `Secret` which holds the password.")
    public abstract PasswordSecretSource getPasswordSecret();

    public abstract void setPasswordSecret(PasswordSecretSource passwordSecret);

    @Description("Username used for the authentication.")
    public abstract String getUsername();

    public abstract void setUsername(String username);
}
