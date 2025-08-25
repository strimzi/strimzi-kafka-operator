/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaClientAuthenticationCustomConfigurationTest {
    @Test
    public void testAllowAndForbiddenConfigurationOptions() {
        JsonObject config = new JsonObject()
                .put("ssl.keystore.location", "/mnt/certs/keystore")
                .put("ssl.truststore.location", "/mnt/certs/keystore")
                .put("sasl.mechanism", "AWS_MSK_IAM")
                .put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
                .put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
                .put("advertised.listeners", "aaa")
                .put("var1", "bbb");

        KafkaClientAuthenticationCustomConfiguration configuration = new KafkaClientAuthenticationCustomConfiguration(Reconciliation.DUMMY_RECONCILIATION, config);

        Map<String, String> properties = configuration.asOrderedProperties().asMap();
        assertThat(properties.size(), is(4));
        assertThat(properties.get("ssl.keystore.location"), is("/mnt/certs/keystore"));
        assertThat(properties.get("sasl.mechanism"), is("AWS_MSK_IAM"));
        assertThat(properties.get("sasl.jaas.config"), is("software.amazon.msk.auth.iam.IAMLoginModule required;"));
        assertThat(properties.get("sasl.client.callback.handler.class"), is("software.amazon.msk.auth.iam.IAMClientCallbackHandler"));

        assertThat(configuration.getConfiguration(), is("""
                ssl.keystore.location=/mnt/certs/keystore
                sasl.mechanism=AWS_MSK_IAM
                sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
                sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
                """));
    }
}
