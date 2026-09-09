/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.auth;

import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RequestedServiceAccountAuthIdentityTest {
    @Test
    public void testClientConfiguration() {
        Map<String, String> expectedClientProperties = Map.of("sasl.mechanism", "OAUTHBEARER",
                "sasl.login.callback.handler.class", "io.strimzi.operator.cluster.auth.KubernetesRequestedServiceAccountTokenLoginCallbackHandler",
                "sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
                        + "strimzi.kubernetes.token.namespace=\"namespace\" "
                        + "strimzi.kubernetes.token.serviceaccount=\"name-cluster-operator\" "
                        + "strimzi.kubernetes.token.audience=\"strimzi.io/kafka/namespace/name\" "
                        + "strimzi.kubernetes.token.expiration.seconds=\"1800\";");

        RequestedServiceAccountAuthIdentity authIdentity = new RequestedServiceAccountAuthIdentity(Reconciliation.DUMMY_RECONCILIATION, "strimzi.io/kafka/namespace/name", 1800L);
        assertThat(authIdentity.isSasl(), is(true));
        assertThat(authIdentity.kafkaClientProperties(), is(expectedClientProperties));
    }

    @Test
    public void testTokenRequestDetails() {
        RequestedServiceAccountAuthIdentity authIdentity = new RequestedServiceAccountAuthIdentity(Reconciliation.DUMMY_RECONCILIATION, "strimzi.io/kafka/namespace/name", 1800L);

        assertThat(authIdentity.namespace(), is("namespace"));
        assertThat(authIdentity.serviceAccountName(), is("name-cluster-operator"));
        assertThat(authIdentity.audience(), is("strimzi.io/kafka/namespace/name"));
        assertThat(authIdentity.expirationSeconds(), is(1800L));
    }
}
