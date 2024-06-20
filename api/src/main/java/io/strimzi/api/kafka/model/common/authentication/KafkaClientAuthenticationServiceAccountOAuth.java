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

/**
 * Configures the Kafka client authentication using SASL OAUTHBEARER mechanism configured to authenticate with the Kubernetes provided token
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
// Make sure this is in sync with KafkaClientAuthenticationOAuth
@JsonPropertyOrder({"type", "clientId", "username", "scope", "audience", "tokenEndpointUri", "connectTimeoutSeconds",
    "readTimeoutSeconds", "httpRetries", "httpRetryPauseMs", "clientSecret", "passwordSecret", "accessToken",
    "refreshToken", "tlsTrustedCertificates", "disableTlsHostnameVerification", "maxTokenExpirySeconds",
    "accessTokenIsJwt", "enableMetrics", "includeAcceptHeader", "accessTokenLocation"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaClientAuthenticationServiceAccountOAuth extends KafkaClientAuthenticationOAuth {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_SERVICEACCOUNT_OAUTH = "serviceaccount-oauth";

    @Description("Must be `" + TYPE_SERVICEACCOUNT_OAUTH + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_SERVICEACCOUNT_OAUTH;
    }
}
