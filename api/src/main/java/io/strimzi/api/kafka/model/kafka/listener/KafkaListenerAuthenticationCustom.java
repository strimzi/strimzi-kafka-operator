/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.GenericSecretSource;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.Map;

/**
 * Configures a listener to use custom authentication.
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "sasl", "listenerConfig", "secrets"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaListenerAuthenticationCustom extends KafkaListenerAuthentication {

    public static final String FORBIDDEN_PREFIXES = "ssl.";

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

    @Description("Configuration to be used for a specific listener. All values are prefixed with listener.name._<listener_name>_.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Map<String, Object> getListenerConfig() {
        return listenerConfig;
    }

    public void setListenerConfig(Map<String, Object> config) {
        this.listenerConfig = config;
    }

    @Description("Secrets to be mounted to /opt/kafka/custom-authn-secrets/custom-listener-_<listener_name>-<port>_/_<secret_name>_")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<GenericSecretSource> getSecrets() {
        return secrets;
    }

    public void setSecrets(List<GenericSecretSource> secrets) {
        this.secrets = secrets;
    }
}
