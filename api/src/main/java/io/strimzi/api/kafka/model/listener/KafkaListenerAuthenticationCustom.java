/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.GenericSecretSource;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
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

    private static final Logger LOGGER = LogManager.getLogger(KafkaListenerAuthenticationCustom.class);

    public static final List<String> FORBIDDEN_PREFIXES = List.of("ssl.");

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

    @Description("Secrets to be mounted to /opt/kafka/custom-auth-secrets/custom-listener-_<listener_name>-<port>_/_<secret_name>_")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<GenericSecretSource> getSecrets() {
        return secrets;
    }

    public void setSecrets(List<GenericSecretSource> secrets) {
        this.secrets = secrets;
    }


    public Map<String, Object> getFilteredListenerConfig() {
        if (listenerConfig == null) {
            return listenerConfig;
        }
        HashMap<String, Object> filteredConfig = new HashMap<>();
        listenerConfig.forEach((key, value) -> {
            if (key != null) {
                String cleanedKey = key.toLowerCase().trim();
                if (FORBIDDEN_PREFIXES.stream().anyMatch(prefix -> cleanedKey.startsWith(prefix.toLowerCase()))) {
                    LOGGER.warn("Configuration option is forbidden \"{}\" and will not be adding key.", key);
                    return;
                }
                LOGGER.trace("Configuration value is valid \"{}\"  and will be passed through to assembly.", key);
                filteredConfig.put(key, value);
            }
        });
        return Map.copyOf(filteredConfig);
    }
}
