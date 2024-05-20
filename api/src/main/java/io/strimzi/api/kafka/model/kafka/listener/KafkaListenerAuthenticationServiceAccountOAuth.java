/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
// Make sure this is in sync with KafkaListenerAuthenticationOAuth
@JsonPropertyOrder({"type", "clientId", "clientSecret", "validIssuerUri", "checkIssuer", "checkAudience",
    "jwksEndpointUri", "jwksRefreshSeconds", "jwksMinRefreshPauseSeconds", "jwksExpirySeconds", "jwksIgnoreKeyUse",
    "introspectionEndpointUri", "userNameClaim", "fallbackUserNameClaim", "fallbackUserNamePrefix",
    "groupsClaim", "groupsClaimDelimiter", "userInfoEndpointUri", "checkAccessTokenType", "validTokenType",
    "accessTokenIsJwt", "tlsTrustedCertificates", "disableTlsHostnameVerification", "enableECDSA",
    "maxSecondsWithoutReauthentication", "enablePlain", "tokenEndpointUri", "enableOauthBearer", "customClaimCheck",
    "connectTimeoutSeconds", "readTimeoutSeconds", "httpRetries", "httpRetryPauseMs", "clientScope", "clientAudience",
    "enableMetrics", "failFast", "includeAcceptHeader", "serverBearerTokenLocation"})
@EqualsAndHashCode(callSuper = true)
public class KafkaListenerAuthenticationServiceAccountOAuth extends KafkaListenerAuthenticationOAuth {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_SERVICEACCOUNT_OAUTH = "serviceaccount-oauth";

    @Description("Must be `" + TYPE_SERVICEACCOUNT_OAUTH + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_SERVICEACCOUNT_OAUTH;
    }

}
