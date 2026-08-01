/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityEncryptionType;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityStatus;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityStatusBuilder;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaClusterSecurityContextTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";

    private static final Map<String, Object> VALID_STATUS = Map.of(
            "encryption", Map.of("type", "strimzi-tls"),
            "authentication", Map.of("type", "strimzi-mtls")
    );

    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build();

    //////////////////////////////////////////////////
    // Tests for the fromCrd method
    //////////////////////////////////////////////////

    @Test
    public void testFromCrdWithoutStatus()  {
        ClusterSecurityStatus status = KafkaClusterSecurityContext.fromCrd(KAFKA).toStatus();

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.STRIMZI_TLS));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.STRIMZI_MTLS));
    }

    @Test
    public void testFromCrdWithStatusWithoutClusterSecurity()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .withObservedGeneration(1L)
                .endStatus()
                .build();

        ClusterSecurityStatus status = KafkaClusterSecurityContext.fromCrd(kafka).toStatus();

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.STRIMZI_TLS));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.STRIMZI_MTLS));
    }

    @Test
    public void testFromCrdWithUntypedClusterSecurityInStatus()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .withClusterSecurity(VALID_STATUS)
                .endStatus()
                .build();

        ClusterSecurityStatus status = KafkaClusterSecurityContext.fromCrd(kafka).toStatus();

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.STRIMZI_TLS));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.STRIMZI_MTLS));
    }

    @Test
    public void testFromCrdWithTypedClusterSecurityInStatus()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .withClusterSecurity(new ClusterSecurityStatusBuilder()
                            .withNewEncryption()
                                .withType(ClusterSecurityEncryptionType.STRIMZI_TLS)
                            .endEncryption()
                            .withNewAuthentication()
                                .withType(ClusterSecurityAuthenticationType.STRIMZI_MTLS)
                            .endAuthentication()
                            .build())
                .endStatus()
                .build();

        ClusterSecurityStatus status = KafkaClusterSecurityContext.fromCrd(kafka).toStatus();

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.STRIMZI_TLS));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.STRIMZI_MTLS));
    }

    @Test
    public void testFromCrdWithInvalidClusterSecurityInStatus()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .withClusterSecurity(Map.of("encryption", Map.of("type", "strimzi-tls")))
                .endStatus()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.fromCrd(kafka));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    //////////////////////////////////////////////////
    // Tests for the deserializeStatus method
    //////////////////////////////////////////////////

    @Test
    public void testDeserializeStatus()  {
        ClusterSecurityStatus status = KafkaClusterSecurityContext.deserializeStatus(VALID_STATUS);

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.STRIMZI_TLS));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.STRIMZI_MTLS));
    }

    @Test
    public void testDeserializeStatusWithUnknownFields()  {
        ClusterSecurityStatus status = KafkaClusterSecurityContext.deserializeStatus(Map.of(
                "encryption", Map.of("type", "strimzi-tls", "someEncryptionField", "someEncryptionValue"),
                "authentication", Map.of("type", "strimzi-mtls"),
                "someField", "someValue"
        ));

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.STRIMZI_TLS));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.STRIMZI_MTLS));
        assertThat(status.getAdditionalProperties(), is(Map.of("someField", "someValue")));
        assertThat(status.getEncryption().getAdditionalProperties(), is(Map.of("someEncryptionField", "someEncryptionValue")));
    }

    @Test
    public void testDeserializeNullStatus()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(null));
        assertThat(e.getMessage(), is("ClusterSecurityStatus is null and cannot be deserialized."));
    }

    @Test
    public void testDeserializeEmptyStatus()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of()));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusWithMissingEncryption()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of("authentication", Map.of("type", "strimzi-mtls"))));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusWithMissingAuthentication()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of("encryption", Map.of("type", "strimzi-tls"))));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusWithMissingEncryptionType()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of(
                "encryption", Map.of(),
                "authentication", Map.of("type", "strimzi-mtls")
        )));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusWithMissingAuthenticationType()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of(
                "encryption", Map.of("type", "strimzi-tls"),
                "authentication", Map.of()
        )));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusWithUnsupportedEncryptionType()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of(
                "encryption", Map.of("type", "some-other-tls"),
                "authentication", Map.of("type", "strimzi-mtls")
        )));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusWithUnsupportedAuthenticationType()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of(
                "encryption", Map.of("type", "strimzi-tls"),
                "authentication", Map.of("type", "some-other-mtls")
        )));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusFromUnsupportedType()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus("strimzi-tls"));
        assertThat(e.getMessage(), is("Failed to deserialize ClusterSecurityStatus"));
        assertThat(e.getCause(), is(notNullValue()));
        assertThat(e.getCause(), is(instanceOf(IllegalArgumentException.class)));
    }

    @Test
    public void testDeserializeStatusFromList()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(List.of(VALID_STATUS)));
        assertThat(e.getMessage(), is("Failed to deserialize ClusterSecurityStatus"));
        assertThat(e.getCause(), is(instanceOf(IllegalArgumentException.class)));
    }

    //////////////////////////////////////////////////
    // Tests for the toStatus method
    //////////////////////////////////////////////////

    @Test
    public void testToStatus()  {
        ClusterSecurityStatus status = KafkaClusterSecurityContext.fromCrd(KAFKA).toStatus();

        assertThat(status, is(new ClusterSecurityStatusBuilder()
                .withNewEncryption()
                    .withType(ClusterSecurityEncryptionType.STRIMZI_TLS)
                .endEncryption()
                .withNewAuthentication()
                    .withType(ClusterSecurityAuthenticationType.STRIMZI_MTLS)
                .endAuthentication()
                .build()));
    }

    @Test
    public void testStatusRoundTrip()  {
        ClusterSecurityStatus status = KafkaClusterSecurityContext.fromCrd(KAFKA).toStatus();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .withClusterSecurity(status)
                .endStatus()
                .build();

        assertThat(KafkaClusterSecurityContext.fromCrd(kafka).toStatus(), is(status));
    }
}
