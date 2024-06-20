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
@JsonPropertyOrder({"enabled", "timeoutSeconds", "config"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaBridgeConsumerSpec extends KafkaBridgeClientSpec {
    public static final String FORBIDDEN_PREFIXES = "ssl., bootstrap.servers, group.id, sasl., security.";
    public static final String FORBIDDEN_PREFIX_EXCEPTIONS = "ssl.endpoint.identification.algorithm, ssl.cipher.suites, ssl.protocol, ssl.enabled.protocols";

    public static final long HTTP_DEFAULT_TIMEOUT = -1L;

    private boolean enabled = true;
    private long timeoutSeconds = HTTP_DEFAULT_TIMEOUT;

    @Override
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Description("The Kafka consumer configuration used for consumer instances created by the bridge. Properties with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES + " (with the exception of: " + FORBIDDEN_PREFIX_EXCEPTIONS + ").")
    public Map<String, Object> getConfig() {
        return config;
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Description("Whether the HTTP consumer should be enabled or disabled, default is enabled.")
    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Description("The timeout in seconds for deleting inactive consumers, default is -1 (disabled).")
    public long getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(long timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }
}
