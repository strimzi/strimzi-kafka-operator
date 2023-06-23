/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import java.util.List;
import java.util.Objects;

public class JaasConfig {
    public static String config(String moduleName, List<String> options) {
        return moduleName + " required " + String.join(" ", options);
    }

    public static String plain(String username, String password) {
        Objects.requireNonNull(username);
        Objects.requireNonNull(password);
        return config("org.apache.kafka.common.security.plain.PlainLoginModule",
                List.of("username=\"" + username + "\"",
                        "password=\"" + password + "\";"));
    }

    public static String scram(String username, String password) {
        Objects.requireNonNull(username);
        Objects.requireNonNull(password);
        return config("org.apache.kafka.common.security.scram.ScramLoginModule",
                List.of("username=\"" + username + "\"",
                        "password=\"" + password + "\";"));
    }

    public static String oauth(String oauthConfig,
                               String clientSecret,
                               String accessToken,
                               String refreshToken,
                               String passwordSecret,
                               String trustedCertsLocation,
                               String trustedCertsPassword,
                               String trustedCertsType) {
        StringBuilder oauthJaasConfig = new StringBuilder("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ");

        if (oauthConfig != null) {
            oauthJaasConfig.append(oauthConfig);
        }

        if (clientSecret != null) {
            oauthJaasConfig.append(" oauth.client.secret=\"" + clientSecret + "\"");
        }

        if (accessToken != null) {
            oauthJaasConfig.append(" oauth.access.token=\"" + accessToken + "\"");
        }

        if (refreshToken != null) {
            oauthJaasConfig.append(" oauth.refresh.token=\"" + refreshToken + "\"");
        }

        if (passwordSecret != null) {
            oauthJaasConfig.append(" oauth.password.grant.password=\"" + passwordSecret + "\"");
        }

        if (trustedCertsLocation != null) {
            oauthJaasConfig.append(" oauth.ssl.truststore.location=\"" + trustedCertsLocation + "\"");
        }
        if (trustedCertsPassword != null) {
            oauthJaasConfig.append(" oauth.ssl.truststore.password=\"" + trustedCertsPassword + "\"");
        }
        if (trustedCertsType != null) {
            oauthJaasConfig.append(" oauth.ssl.truststore.type=\"" + trustedCertsType + "\"");
        }

        oauthJaasConfig.append(";");

        return oauthJaasConfig.toString();
    }


    // TODO scram
    // TODO oauth
    //
}
