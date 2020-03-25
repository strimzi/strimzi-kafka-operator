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
 * Configures the Kafka client authentication using SASL PLAIN in client based components
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaClientAuthenticationPlain extends KafkaClientAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_PLAIN = "plain";

    private String username;
    private PasswordSecretSource passwordSecret;

    @Description("Must be `" + TYPE_PLAIN + "`")
    @Override
    public String getType() {
        return TYPE_PLAIN;
    }

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
