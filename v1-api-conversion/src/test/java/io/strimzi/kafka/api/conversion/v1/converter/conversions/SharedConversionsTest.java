/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnvBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class SharedConversionsTest extends AbstractConversionsTest {
    @Test
    public void testJaegerTracing() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .withNewSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                .endSpec()
                .build();

        Conversion<KafkaConnect> c = SharedConversions.deleteJaegerTracing();

        c.convert(connect);
        assertThat(connect.getSpec().getTracing(), is(nullValue()));
    }

    @Test
    public void testJaegerTracingJson() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .withNewSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                .endSpec()
                .build();
        JsonNode connectJson = typedToJsonNode(connect);

        Conversion<KafkaConnect> c = SharedConversions.deleteJaegerTracing();
        c.convert(connectJson);

        KafkaConnect converted = jsonNodeToTyped(connectJson, KafkaConnect.class);
        assertThat(converted.getSpec().getTracing(), is(nullValue()));
    }

    @Test
    public void testDefaultReplicas() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .withNewSpec()
                .endSpec()
                .build();

        Conversion<KafkaConnect> c = SharedConversions.defaultReplicas(3);

        c.convert(connect);
        assertThat(connect.getSpec().getReplicas(), is(3));
    }

    @Test
    public void testDefaultReplicasJson() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .withNewSpec()
                .endSpec()
                .build();
        JsonNode connectJson = typedToJsonNode(connect);

        Conversion<KafkaConnect> c = SharedConversions.defaultReplicas(3);
        c.convert(connectJson);

        KafkaConnect converted = jsonNodeToTyped(connectJson, KafkaConnect.class);
        assertThat(converted.getSpec().getReplicas(), is(3));
    }

    @Test
    public void testEnforceOAuth() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .withNewSpec()
                    .withNewKafkaClientAuthenticationOAuth()
                    .endKafkaClientAuthenticationOAuth()
                .endSpec()
                .build();

        Conversion<KafkaConnect> c = SharedConversions.enforceOAuthClientAuthentication();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(connect));
        assertThat(ex.getMessage(), containsString("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceOAuthJson() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .withNewSpec()
                    .withNewKafkaClientAuthenticationOAuth()
                    .endKafkaClientAuthenticationOAuth()
                .endSpec()
                .build();
        JsonNode connectJson = typedToJsonNode(connect);

        Conversion<KafkaConnect> c = SharedConversions.enforceOAuthClientAuthentication();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(connectJson));
        assertThat(ex.getMessage(), containsString("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceExternalConfiguration() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .withNewSpec()
                    .withNewExternalConfiguration()
                        .withEnv(new ExternalConfigurationEnvBuilder().withName("MY_ENV").withNewValueFrom().withNewConfigMapKeyRef("my-key", "my-cm", true).endValueFrom().build())
                    .endExternalConfiguration()
                .endSpec()
                .build();

        Conversion<KafkaConnect> c = SharedConversions.enforceExternalConfiguration();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(connect));
        assertThat(ex.getMessage(), containsString("The External Configuration is removed in the v1 API version. Use the .spec.template section instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceExternalConfigurationJson() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .withNewSpec()
                    .withNewExternalConfiguration()
                        .withEnv(new ExternalConfigurationEnvBuilder().withName("MY_ENV").withNewValueFrom().withNewConfigMapKeyRef("my-key", "my-cm", true).endValueFrom().build())
                    .endExternalConfiguration()
                .endSpec()
                .build();
        JsonNode connectJson = typedToJsonNode(connect);

        Conversion<KafkaConnect> c = SharedConversions.enforceExternalConfiguration();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(connectJson));
        assertThat(ex.getMessage(), containsString("The External Configuration is removed in the v1 API version. Use the .spec.template section instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceSpec() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .build();

        Conversion<KafkaConnect> c = SharedConversions.enforceSpec();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(connect));
        assertThat(ex.getMessage(), containsString("The .spec section is required and has to be added manually to KafkaConnect kind resources. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testSpecJson() {
        KafkaConnect connect = new KafkaConnectBuilder()
                .build();
        JsonNode connectJson = typedToJsonNode(connect);

        Conversion<KafkaConnect> c = SharedConversions.enforceSpec();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(connectJson));
        assertThat(ex.getMessage(), containsString("The .spec section is required and has to be added manually to KafkaConnect kind resources. Please fix the resource manually and re-run the conversion tool."));
    }
}