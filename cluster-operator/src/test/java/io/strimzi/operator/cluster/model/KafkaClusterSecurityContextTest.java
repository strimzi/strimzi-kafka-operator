/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurity;
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
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaClusterSecurityContextTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String INTERNAL_CLUSTER_SECURITY_ANNOTATION = "strimzi.io/internal-cluster-security";
    private static final String TLS_WITHOUT_AUTHENTICATION = "{\"encryption\":{\"type\":\"tls\"},\"authentication\":{\"type\":\"none\"}}";
    private static final String LEGACY_TLS_WITH_MTLS = "{\"encryption\":{\"type\":\"strimzi-tls\"},\"authentication\":{\"type\":\"strimzi-mtls\"}}";
    private static final String WITHOUT_ENCRYPTION_OR_AUTHENTICATION = "{\"encryption\":{\"type\":\"none\"},\"authentication\":{\"type\":\"none\"}}";
    private static final String MTLS_WITHOUT_TLS = "{\"encryption\":{\"type\":\"none\"},\"authentication\":{\"type\":\"mtls\"}}";

    private static final Map<String, Object> VALID_STATUS = Map.of(
            "encryption", Map.of("type", "tls"),
            "authentication", Map.of("type", "mtls")
    );

    private static final Map<String, Object> VALID_LEGACY_STATUS = Map.of(
            "encryption", Map.of("type", "strimzi-tls"),
            "authentication", Map.of("type", "strimzi-mtls")
    );

    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build();

    private static ClusterSecurityStatus status(ClusterSecurityEncryptionType encryption, ClusterSecurityAuthenticationType authentication) {
        return new ClusterSecurityStatusBuilder()
                .withNewEncryption()
                    .withType(encryption)
                .endEncryption()
                .withNewAuthentication()
                    .withType(authentication)
                .endAuthentication()
                .build();
    }

    private static Kafka kafkaWithClusterSecurity(String clusterSecurity, ClusterSecurityStatus clusterSecurityStatus) {
        KafkaBuilder builder = new KafkaBuilder(KAFKA)
                .editOrNewMetadata()
                    .addToAnnotations(INTERNAL_CLUSTER_SECURITY_ANNOTATION, clusterSecurity)
                .endMetadata();

        if (clusterSecurityStatus != null) {
            builder.withNewStatus()
                    .withClusterSecurity(clusterSecurityStatus)
                    .endStatus();
        }

        return builder.build();
    }

    //////////////////////////////////////////////////
    // Tests for the fromCrd method
    //////////////////////////////////////////////////

    @Test
    public void testFromCrdWithoutStatus()  {
        ClusterSecurityStatus status = KafkaClusterSecurityContext.fromCrd(KAFKA).toStatus();

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.TLS));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.MTLS));
    }

    @Test
    public void testFromCrdWithStatusWithoutClusterSecurity()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .withObservedGeneration(1L)
                .endStatus()
                .build();

        ClusterSecurityStatus status = KafkaClusterSecurityContext.fromCrd(kafka).toStatus();

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.TLS));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.MTLS));
    }

    @Test
    public void testFromCrdWithUntypedClusterSecurityInStatus()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .withClusterSecurity(VALID_STATUS)
                .endStatus()
                .build();

        ClusterSecurityStatus status = KafkaClusterSecurityContext.fromCrd(kafka).toStatus();

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.TLS));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.MTLS));
    }

    @Test
    public void testFromCrdWithTypedClusterSecurityInStatus()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .withClusterSecurity(new ClusterSecurityStatusBuilder()
                            .withNewEncryption()
                                .withType(ClusterSecurityEncryptionType.TLS)
                            .endEncryption()
                            .withNewAuthentication()
                                .withType(ClusterSecurityAuthenticationType.MTLS)
                            .endAuthentication()
                            .build())
                .endStatus()
                .build();

        ClusterSecurityStatus status = KafkaClusterSecurityContext.fromCrd(kafka).toStatus();

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.TLS));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.MTLS));
    }

    @Test
    public void testFromCrdWithInvalidClusterSecurityInStatus()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .withClusterSecurity(Map.of("encryption", Map.of("type", "tls")))
                .endStatus()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.fromCrd(kafka));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testFromCrdWithClusterSecurityInAnnotation()  {
        Kafka kafka = kafkaWithClusterSecurity(WITHOUT_ENCRYPTION_OR_AUTHENTICATION, null);

        KafkaClusterSecurityContext context = KafkaClusterSecurityContext.fromCrd(kafka);

        assertThat(context.isTlsEncryption(), is(false));
        assertThat(context.isMtlsAuthentication(), is(false));
        assertThat(context.toStatus(), is(status(ClusterSecurityEncryptionType.NONE, ClusterSecurityAuthenticationType.NONE)));
    }

    @Test
    public void testFromCrdWithMatchingClusterSecurityInAnnotationAndStatus()  {
        ClusterSecurityStatus status = status(ClusterSecurityEncryptionType.TLS, ClusterSecurityAuthenticationType.NONE);
        Kafka kafka = kafkaWithClusterSecurity(TLS_WITHOUT_AUTHENTICATION, status);

        KafkaClusterSecurityContext context = KafkaClusterSecurityContext.fromCrd(kafka);

        assertThat(context.isTlsEncryption(), is(true));
        assertThat(context.isMtlsAuthentication(), is(false));
        assertThat(context.toStatus(), is(status));
    }

    @Test
    public void testFromCrdWithMismatchingClusterSecurityInAnnotationAndStatus()  {
        ClusterSecurityStatus status = status(ClusterSecurityEncryptionType.NONE, ClusterSecurityAuthenticationType.NONE);
        Kafka kafka = kafkaWithClusterSecurity(TLS_WITHOUT_AUTHENTICATION, status);

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.fromCrd(kafka));
        assertThat(e.getMessage(), is("Desired Cluster Security configuration does not match the current configuration. " +
                "If you want to change the Cluster Security configuration, please follow the documentation."));
    }

    @Test
    public void testFromCrdWithNonDefaultStatusWithoutAnnotation()  {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .withClusterSecurity(status(ClusterSecurityEncryptionType.TLS, ClusterSecurityAuthenticationType.NONE))
                .endStatus()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.fromCrd(kafka));
        assertThat(e.getMessage(), is("Desired Cluster Security configuration does not match the current configuration. " +
                "If you want to change the Cluster Security configuration, please follow the documentation."));
    }

    @Test
    public void testFromCrdWithMtlsWithoutTls()  {
        Kafka kafka = kafkaWithClusterSecurity(MTLS_WITHOUT_TLS, null);

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.fromCrd(kafka));
        assertThat(e.getMessage(), is("Desired Cluster Security configuration is not valid: mTLS authentication can be used only with enabled TLS encryption."));
    }

    //////////////////////////////////////////////////
    // Tests for the constructor and type helpers
    //////////////////////////////////////////////////

    @Test
    public void testTlsAndMtls()  {
        KafkaClusterSecurityContext context = new KafkaClusterSecurityContext(ClusterSecurityEncryptionType.TLS, ClusterSecurityAuthenticationType.MTLS);

        assertThat(context.isTlsEncryption(), is(true));
        assertThat(context.isMtlsAuthentication(), is(true));
        assertThat(context.toStatus(), is(status(ClusterSecurityEncryptionType.TLS, ClusterSecurityAuthenticationType.MTLS)));
    }

    @Test
    public void testTlsWithoutAuthentication()  {
        KafkaClusterSecurityContext context = new KafkaClusterSecurityContext(ClusterSecurityEncryptionType.TLS, ClusterSecurityAuthenticationType.NONE);

        assertThat(context.isTlsEncryption(), is(true));
        assertThat(context.isMtlsAuthentication(), is(false));
        assertThat(context.toStatus(), is(status(ClusterSecurityEncryptionType.TLS, ClusterSecurityAuthenticationType.NONE)));
    }

    @Test
    public void testWithoutEncryptionOrAuthentication()  {
        KafkaClusterSecurityContext context = new KafkaClusterSecurityContext(ClusterSecurityEncryptionType.NONE, ClusterSecurityAuthenticationType.NONE);

        assertThat(context.isTlsEncryption(), is(false));
        assertThat(context.isMtlsAuthentication(), is(false));
        assertThat(context.toStatus(), is(status(ClusterSecurityEncryptionType.NONE, ClusterSecurityAuthenticationType.NONE)));
    }

    @Test
    public void testMtlsWithoutTlsIsInvalid()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () ->
                new KafkaClusterSecurityContext(ClusterSecurityEncryptionType.NONE, ClusterSecurityAuthenticationType.MTLS));

        assertThat(e.getMessage(), is("Desired Cluster Security configuration is not valid: mTLS authentication can be used only with enabled TLS encryption."));
    }

    //////////////////////////////////////////////////
    // Tests for the deserializeSpec method
    //////////////////////////////////////////////////

    @Test
    public void testDeserializeSpec()  {
        ClusterSecurity clusterSecurity = KafkaClusterSecurityContext.deserializeSpec(WITHOUT_ENCRYPTION_OR_AUTHENTICATION);

        assertThat(clusterSecurity.getEncryption().getType(), is(ClusterSecurityEncryptionType.NONE));
        assertThat(clusterSecurity.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.NONE));
    }

    @Test
    public void testDeserializeLegacySpec()  {
        ClusterSecurity clusterSecurity = KafkaClusterSecurityContext.deserializeSpec(LEGACY_TLS_WITH_MTLS);

        assertThat(clusterSecurity.getEncryption().getType(), is(ClusterSecurityEncryptionType.TLS));
        assertThat(clusterSecurity.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.MTLS));
    }

    @Test
    public void testDeserializeSpecWithUnknownFields()  {
        ClusterSecurity clusterSecurity = KafkaClusterSecurityContext.deserializeSpec("""
                {
                    "encryption": {"type": "tls", "someEncryptionField": "someEncryptionValue"},
                    "authentication": {"type": "none"},
                    "someField": "someValue"
                }
                """);

        assertThat(clusterSecurity.getEncryption().getType(), is(ClusterSecurityEncryptionType.TLS));
        assertThat(clusterSecurity.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.NONE));
        assertThat(clusterSecurity.getAdditionalProperties(), is(Map.of("someField", "someValue")));
        assertThat(clusterSecurity.getEncryption().getAdditionalProperties(), is(Map.of("someEncryptionField", "someEncryptionValue")));
    }

    @Test
    public void testDeserializeNullSpec()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeSpec(null));
        assertThat(e.getMessage(), is("ClusterSecurity is null and cannot be deserialized."));
    }

    @Test
    public void testDeserializeInvalidSpec()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeSpec("not-json"));
        assertThat(e.getMessage(), is("Failed to deserialize ClusterSecurity configuration"));
        assertThat(e.getCause(), is(notNullValue()));
    }

    //////////////////////////////////////////////////
    // Tests for the deserializeStatus method
    //////////////////////////////////////////////////

    @Test
    public void testDeserializeStatus()  {
        ClusterSecurityStatus status = KafkaClusterSecurityContext.deserializeStatus(VALID_STATUS);

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.TLS));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.MTLS));
    }

    @Test
    public void testDeserializeLegacyStatus()  {
        ClusterSecurityStatus status = KafkaClusterSecurityContext.deserializeStatus(VALID_LEGACY_STATUS);

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.TLS));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.MTLS));
    }

    @Test
    public void testDeserializeStatusWithoutEncryptionOrAuthentication()  {
        ClusterSecurityStatus status = KafkaClusterSecurityContext.deserializeStatus(Map.of(
                "encryption", Map.of("type", "none"),
                "authentication", Map.of("type", "none")
        ));

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.NONE));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.NONE));
    }

    @Test
    public void testDeserializeStatusWithUnknownFields()  {
        ClusterSecurityStatus status = KafkaClusterSecurityContext.deserializeStatus(Map.of(
                "encryption", Map.of("type", "tls", "someEncryptionField", "someEncryptionValue"),
                "authentication", Map.of("type", "mtls"),
                "someField", "someValue"
        ));

        assertThat(status.getEncryption().getType(), is(ClusterSecurityEncryptionType.TLS));
        assertThat(status.getAuthentication().getType(), is(ClusterSecurityAuthenticationType.MTLS));
        assertThat(status.getAdditionalProperties(), is(Map.of("someField", "someValue")));
        assertThat(status.getEncryption().getAdditionalProperties(), is(Map.of("someEncryptionField", "someEncryptionValue")));
    }

    @Test
    public void testDeserializeNullStatus()  {
        assertThat(KafkaClusterSecurityContext.deserializeStatus(null), is(nullValue()));
    }

    @Test
    public void testDeserializeEmptyStatus()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of()));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusWithMissingEncryption()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of("authentication", Map.of("type", "mtls"))));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusWithMissingAuthentication()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of("encryption", Map.of("type", "tls"))));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusWithMissingEncryptionType()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of(
                "encryption", Map.of(),
                "authentication", Map.of("type", "mtls")
        )));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusWithMissingAuthenticationType()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of(
                "encryption", Map.of("type", "tls"),
                "authentication", Map.of()
        )));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusWithUnsupportedEncryptionType()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of(
                "encryption", Map.of("type", "some-other-tls"),
                "authentication", Map.of("type", "mtls")
        )));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusWithUnsupportedAuthenticationType()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus(Map.of(
                "encryption", Map.of("type", "tls"),
                "authentication", Map.of("type", "some-other-mtls")
        )));
        assertThat(e.getMessage(), is("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set"));
    }

    @Test
    public void testDeserializeStatusFromUnsupportedType()  {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> KafkaClusterSecurityContext.deserializeStatus("tls"));
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
                    .withType(ClusterSecurityEncryptionType.TLS)
                .endEncryption()
                .withNewAuthentication()
                    .withType(ClusterSecurityAuthenticationType.MTLS)
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
