/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.common.GenericSecretSource;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatusBuilder;
import io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("deprecation")
class KafkaConversionsTest extends AbstractConversionsTest {
    @Test
    public void testEnforceOAuth() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withNewKafkaListenerAuthenticationOAuth().endKafkaListenerAuthenticationOAuth().build())
                    .endKafka()
                .endSpec()
                .build();

        Conversion<Kafka> c = KafkaConversions.enforceOauthServerAuthentication();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(kafka));
        assertThat(ex.getMessage(), containsString("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceOAuthJson() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withNewKafkaListenerAuthenticationOAuth().endKafkaListenerAuthenticationOAuth().build())
                    .endKafka()
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(kafka);

        Conversion<Kafka> c = KafkaConversions.enforceOauthServerAuthentication();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(json));
        assertThat(ex.getMessage(), containsString("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceKeycloakAuthorization() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewKafka()
                        .withNewKafkaAuthorizationKeycloak()
                        .endKafkaAuthorizationKeycloak()
                    .endKafka()
                .endSpec()
                .build();

        Conversion<Kafka> c = KafkaConversions.enforceKeycloakAuthorization();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(kafka));
        assertThat(ex.getMessage(), containsString("The Keycloak authorization is removed in the v1 API version. Use the Custom authorization instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceKeycloakAuthorizationJson() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewKafka()
                        .withNewKafkaAuthorizationKeycloak()
                        .endKafkaAuthorizationKeycloak()
                    .endKafka()
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(kafka);

        Conversion<Kafka> c = KafkaConversions.enforceKeycloakAuthorization();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(json));
        assertThat(ex.getMessage(), containsString("The Keycloak authorization is removed in the v1 API version. Use the Custom authorization instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceOpaAuthorization() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewKafka()
                        .withNewKafkaAuthorizationOpa()
                        .endKafkaAuthorizationOpa()
                    .endKafka()
                .endSpec()
                .build();

        Conversion<Kafka> c = KafkaConversions.enforceOpaAuthorization();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(kafka));
        assertThat(ex.getMessage(), containsString("The Open Policy Agent authorization is removed in the v1 API version. Use the Custom authorization instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceOpaAuthorizationJson() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewKafka()
                        .withNewKafkaAuthorizationOpa()
                        .endKafkaAuthorizationOpa()
                    .endKafka()
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(kafka);

        Conversion<Kafka> c = KafkaConversions.enforceOpaAuthorization();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(json));
        assertThat(ex.getMessage(), containsString("The Open Policy Agent authorization is removed in the v1 API version. Use the Custom authorization instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceCustomAuthorizationSecrets() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withNewKafkaListenerAuthenticationCustomAuth().withSecrets(List.of(new GenericSecretSource())).endKafkaListenerAuthenticationCustomAuth().build())
                    .endKafka()
                .endSpec()
                .build();

        Conversion<Kafka> c = KafkaConversions.enforceSecretsInCustomServerAuthentication();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(kafka));
        assertThat(ex.getMessage(), containsString("Adding secrets in custom authentication is removed in the v1 API version. Use the additional volumes feature in the template section instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceCustomAuthorizationSecretsJson() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withNewKafkaListenerAuthenticationCustomAuth().withSecrets(List.of(new GenericSecretSource())).endKafkaListenerAuthenticationCustomAuth().build())
                    .endKafka()
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(kafka);

        Conversion<Kafka> c = KafkaConversions.enforceSecretsInCustomServerAuthentication();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(json));
        assertThat(ex.getMessage(), containsString("Adding secrets in custom authentication is removed in the v1 API version. Use the additional volumes feature in the template section instead. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceResources() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewKafka()
                        .withResources(new ResourceRequirementsBuilder().withLimits(Map.of("memory", new Quantity("16Gi"), "cpu", new Quantity("8"))).build())
                    .endKafka()
                .endSpec()
                .build();

        Conversion<Kafka> c = KafkaConversions.enforceKafkaResources();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(kafka));
        assertThat(ex.getMessage(), containsString("The resources configuration in .spec.kafka.resources is removed in the v1 API version. Use the KafkaNodePool resource instead to configure resources. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testEnforceResourcesJson() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewKafka()
                        .withResources(new ResourceRequirementsBuilder().withLimits(Map.of("memory", new Quantity("16Gi"), "cpu", new Quantity("8"))).build())
                    .endKafka()
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(kafka);

        Conversion<Kafka> c = KafkaConversions.enforceKafkaResources();

        ApiConversionFailedException ex = Assertions.assertThrows(ApiConversionFailedException.class, () -> c.convert(json));
        assertThat(ex.getMessage(), containsString("The resources configuration in .spec.kafka.resources is removed in the v1 API version. Use the KafkaNodePool resource instead to configure resources. Please fix the resource manually and re-run the conversion tool."));
    }

    @Test
    public void testToReconciliationInterval() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                            .withReconciliationIntervalSeconds(13)
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        Conversion<Kafka> c = KafkaConversions.convertReconciliationIntervalInTO();
        c.convert(kafka);

        assertThat(kafka.getSpec().getEntityOperator().getTopicOperator().getReconciliationIntervalSeconds(), is(nullValue()));
        assertThat(kafka.getSpec().getEntityOperator().getTopicOperator().getReconciliationIntervalMs(), is(13000L));
    }

    @Test
    public void testToReconciliationIntervalJson() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                            .withReconciliationIntervalSeconds(13)
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(kafka);

        Conversion<Kafka> c = KafkaConversions.convertReconciliationIntervalInTO();
        c.convert(json);

        Kafka converted = jsonNodeToTyped(json, Kafka.class);
        assertThat(converted.getSpec().getEntityOperator().getTopicOperator().getReconciliationIntervalSeconds(), is(nullValue()));
        assertThat(converted.getSpec().getEntityOperator().getTopicOperator().getReconciliationIntervalMs(), is(13000L));
    }

    @Test
    public void testUoReconciliationInterval() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                            .withReconciliationIntervalSeconds(13L)
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        Conversion<Kafka> c = KafkaConversions.convertReconciliationIntervalInUO();
        c.convert(kafka);

        assertThat(kafka.getSpec().getEntityOperator().getUserOperator().getReconciliationIntervalSeconds(), is(nullValue()));
        assertThat(kafka.getSpec().getEntityOperator().getUserOperator().getReconciliationIntervalMs(), is(13000L));
    }

    @Test
    public void testUoReconciliationIntervalJson() {
        Kafka kafka = new KafkaBuilder()
                .withNewSpec()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                            .withReconciliationIntervalSeconds(13L)
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(kafka);

        Conversion<Kafka> c = KafkaConversions.convertReconciliationIntervalInUO();
        c.convert(json);

        Kafka converted = jsonNodeToTyped(json, Kafka.class);
        assertThat(converted.getSpec().getEntityOperator().getUserOperator().getReconciliationIntervalSeconds(), is(nullValue()));
        assertThat(converted.getSpec().getEntityOperator().getUserOperator().getReconciliationIntervalMs(), is(13000L));
    }

    @Test
    public void testStatusListenerType() {
        Kafka kafka = new KafkaBuilder()
                .withNewStatus()
                    .withListeners(new ListenerStatusBuilder().withName("plain").withType("plain").build())
                .endStatus()
                .build();

        Conversion<Kafka> c = KafkaConversions.removeListenerTypeInStatus();
        c.convert(kafka);

        assertThat(kafka.getStatus().getListeners().get(0).getType(), is(nullValue()));
        assertThat(kafka.getStatus().getListeners().get(0).getName(), is("plain"));
    }

    @Test
    public void testStatusListenerTypeJson() {
        Kafka kafka = new KafkaBuilder()
                .withNewStatus()
                    .withListeners(new ListenerStatusBuilder().withName("plain").withType("plain").build())
                .endStatus()
                .build();
        JsonNode json = typedToJsonNode(kafka);

        Conversion<Kafka> c = KafkaConversions.removeListenerTypeInStatus();
        c.convert(json);

        Kafka converted = jsonNodeToTyped(json, Kafka.class);
        assertThat(converted.getStatus().getListeners().get(0).getType(), is(nullValue()));
        assertThat(converted.getStatus().getListeners().get(0).getName(), is("plain"));
    }
}