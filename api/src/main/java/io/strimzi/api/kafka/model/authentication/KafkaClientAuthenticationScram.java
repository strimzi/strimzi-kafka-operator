/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.authentication;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.crdgenerator.annotations.Description;
import lombok.EqualsAndHashCode;

/**
 * Configures the Kafka client authentication using one of the possible SASL SCRAM_SHA_* methods in client based
 * components
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode(callSuper = true)
public abstract class KafkaClientAuthenticationScram extends KafkaClientAuthentication {
    private static final long serialVersionUID = 1L;

    private String username;
    private PasswordSecretSource passwordSecret;

    @Description("Reference to the `Secret` which holds the password.")
    public PasswordSecretSource getPasswordSecret() {
        return passwordSecret;
    }

    public void setPasswordSecret(PasswordSecretSource passwordSecret) {
        this.passwordSecret = passwordSecret;
    }

    @Description("Username used for the authentication.")
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
