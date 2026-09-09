/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.auth;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ProjectedServiceAccountAuthIdentityTest {
    private static final String TOKEN_PATH = "/var/run/secrets/kafka/serviceaccount/token";

    @Test
    public void testClientConfiguration() {
        Map<String, String> expectedClientProperties = Map.of("sasl.mechanism", "OAUTHBEARER",
                "sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                "sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                        "oauth.access.token.location=\"" + TOKEN_PATH + "\";");

        ProjectedServiceAccountAuthIdentity authIdentity = new ProjectedServiceAccountAuthIdentity(TOKEN_PATH);
        assertThat(authIdentity.isSasl(), is(true));
        assertThat(authIdentity.kafkaClientProperties(), is(expectedClientProperties));
    }
}
