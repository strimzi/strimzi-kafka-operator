/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.GenericSecretSource;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

/**
 * Configures a listener to use mutual TLS authentication.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaListenerAuthenticationCustom extends KafkaListenerAuthentication {

    public static final String FORBIDDEN_PREFIXES = "ssl.keystore.location, ssl.keystore.password, ssl.keystore.type";

    private static final long serialVersionUID = 1L;

    public static final String TYPE_CUSTOM = "custom";

    private Map<String, Object> listenerConfig;
    private boolean sasl;
    private List<GenericSecretSource> secrets;

    @Description("Must be `" + TYPE_CUSTOM + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_CUSTOM;
    }

    @Description("Enable or disable SASL on this listener.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public boolean isSasl() {
        return sasl;
    }

    public void setSasl(boolean enabled) {
        this.sasl = enabled;
    }

    @Description("Configuration to be used for a specific listener. All values are prefixed with lister.name.<lister-name>.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Map<String, Object> getListenerConfig() {
        return listenerConfig;
    }

    public void setListenerConfig(Map<String, Object> config) {
        this.listenerConfig = config;
    }

    @Description("Secrets to be mounted to /mnt/strimzi/custom-auth-secrets/<listener-name>/<secret-key>")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<GenericSecretSource> getSecrets() {
        return secrets;
    }

    public void setSecrets(List<GenericSecretSource> secrets) {
        this.secrets = secrets;
    }
}
