/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.authentication;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Map;

/**
 * Configures the Kafka client authentication using a custom mechanism in client based components
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "sasl", "config"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaClientAuthenticationCustom extends KafkaClientAuthentication {
    public static final String ALLOWED_PREFIXES = "ssl.keystore., sasl.";
    public static final String TYPE_CUSTOM = "custom";

    private Map<String, Object> config;
    private boolean sasl;

    @Description("Must be `" + TYPE_CUSTOM + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_CUSTOM;
    }

    @Description("Enable or disable SASL on this authentication mechanism.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public boolean isSasl() {
        return sasl;
    }

    public void setSasl(boolean enabled) {
        this.sasl = enabled;
    }

    @Description("Configuration to be used for the custom authentication mechanism. " +
            "Allows are the `sasl.` and `ssl.keystore.` options. " +
            "Other options should be specified in the regular configuration section.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }
}
