/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.certs.OpenSslCertIssuer;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Base64;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CustomCaProviderTest {
    private static final String NAMESPACE = "test";
    private static final String NAME = "my-cluster";
    private static final CaConfig CA_CONFIG = new CaConfig(100, 10, false, false);
    private final static OpenSslCertIssuer CERT_ISSUER = new OpenSslCertIssuer();
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build())
                .endKafka()
            .endSpec()
            .build();

    @Test
    public void testInitCaSecretsWhenUserManagedCertsAreMissingThrows() {
        CustomCaProvider userCaSecretProvider = new CustomCaProvider(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, CA_CONFIG, KAFKA, null, null, null, null);
        Exception exception = assertThrows(InvalidResourceException.class, () -> userCaSecretProvider.createAndReconcileCa().toCompletableFuture().join());
        assertThat(exception.getMessage(), is("Cluster CA should not be generated, but the secrets were not found."));
    }

    @Test
    public void testInitCaSecrets() throws IOException {
        Subject sbj = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName("ca").build();

        File rootKey = Files.createTempFile("key-", ".key").toFile();
        rootKey.deleteOnExit();
        File rootCert = Files.createTempFile("crt-", ".crt").toFile();
        rootCert.deleteOnExit();
        File intermediateKey1 = Files.createTempFile("key-", ".key").toFile();
        intermediateKey1.deleteOnExit();
        File intermediateCert1 = Files.createTempFile("crt-", ".crt").toFile();
        intermediateCert1.deleteOnExit();
        File intermediateKey2 = Files.createTempFile("key-", ".key").toFile();
        intermediateKey2.deleteOnExit();
        File intermediateCert2 = Files.createTempFile("crt-", ".crt").toFile();

        Instant now = Instant.now();
        ZonedDateTime notBefore = now.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ZonedDateTime notAfter = now.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        CERT_ISSUER.generateRootCaCert(sbj, rootKey, rootCert, notBefore, notAfter, 1);

        // Generate an intermediate cert
        Subject intermediateSubject1 = new Subject.Builder().withCommonName("IntermediateCn1").withOrganizationName("MyOrganization").build();
        CERT_ISSUER.generateIntermediateCaCert(rootKey, rootCert, intermediateSubject1, intermediateKey1, intermediateCert1, notBefore, notAfter, 1);

        // Generate an additional intermediate cert
        Subject intermediateSubject2 = new Subject.Builder().withCommonName("IntermediateCn2").withOrganizationName("MyOrganization").build();
        CERT_ISSUER.generateIntermediateCaCert(intermediateKey1, intermediateCert1, intermediateSubject2, intermediateKey2, intermediateCert2, notBefore, notAfter, 1);

        String caKey = Base64.getEncoder().encodeToString(Files.readAllBytes(rootKey.toPath()));

        // Generate combined Pem files with CA chain in correct order
        String validCombinedPem;
        try (FileInputStream int2CertFis = new FileInputStream(intermediateCert2);
             FileInputStream int1CertFis = new FileInputStream(intermediateCert1);
             FileInputStream rootCertFis = new FileInputStream(rootCert)) {
            String combinedPem = String.join("\n",
                    new String(int2CertFis.readAllBytes(), StandardCharsets.US_ASCII),
                    new String(int1CertFis.readAllBytes(), StandardCharsets.US_ASCII),
                    new String(rootCertFis.readAllBytes(), StandardCharsets.US_ASCII));
            validCombinedPem = Base64.getEncoder().encodeToString(combinedPem.getBytes(StandardCharsets.US_ASCII));
        }

        Secret initialClusterCaKeySecret = ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, AbstractModel.clusterCaKeySecretName(NAME), caKey)
                .edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "3")
                .endMetadata()
                .build();
        Secret initialClusterCaCertSecret = ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), validCombinedPem, null, null)
                .edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "5")
                .endMetadata()
                .build();

        CustomCaProvider clusterCaProvider = new CustomCaProvider(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, CA_CONFIG, KAFKA, null, null, initialClusterCaCertSecret, initialClusterCaKeySecret);
        CaProviderResult clusterCaProviderResult = clusterCaProvider.createAndReconcileCa().toCompletableFuture().join();
        Ca clusterCa = clusterCaProviderResult.ca();
        assertThat(clusterCa.caCertGeneration(), is(5));
        assertThat(clusterCa.caKeyGeneration(), is(3));

        assertThat(clusterCaProviderResult.certSecret(), is(initialClusterCaCertSecret));

        Secret initialClientsCaKeySecret = ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, KafkaResources.clientsCaKeySecretName(NAME), caKey);
        Secret initialClientsCaCertSecret = ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), validCombinedPem, null, null);

        CustomCaProvider clientsCaProvider = new CustomCaProvider(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLIENTS_CA, CA_CONFIG, KAFKA, null, null, initialClientsCaCertSecret, initialClientsCaKeySecret);
        CaProviderResult clientsCaProviderResult = clientsCaProvider.createAndReconcileCa().toCompletableFuture().join();
        assertThat(clientsCaProviderResult.certSecret(), is(initialClientsCaCertSecret));
    }

    @Test
    public void testInitCaSecretsWithInvalidCa() throws IOException {
        Subject sbj = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName("ca").build();

        File rootKey = Files.createTempFile("key-", ".key").toFile();
        rootKey.deleteOnExit();
        File rootCert = Files.createTempFile("crt-", ".crt").toFile();
        rootCert.deleteOnExit();
        File intermediateKey1 = Files.createTempFile("key-", ".key").toFile();
        intermediateKey1.deleteOnExit();
        File intermediateCert1 = Files.createTempFile("crt-", ".crt").toFile();
        intermediateCert1.deleteOnExit();
        File intermediateKey2 = Files.createTempFile("key-", ".key").toFile();
        intermediateKey2.deleteOnExit();
        File intermediateCert2 = Files.createTempFile("crt-", ".crt").toFile();

        Instant now = Instant.now();
        ZonedDateTime notBefore = now.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ZonedDateTime notAfter = now.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        CERT_ISSUER.generateRootCaCert(sbj, rootKey, rootCert, notBefore, notAfter, 1);

        // Generate an intermediate cert
        Subject intermediateSubject1 = new Subject.Builder().withCommonName("IntermediateCn1").withOrganizationName("MyOrganization").build();
        CERT_ISSUER.generateIntermediateCaCert(rootKey, rootCert, intermediateSubject1, intermediateKey1, intermediateCert1, notBefore, notAfter, 1);

        // Generate an additional intermediate cert
        Subject intermediateSubject2 = new Subject.Builder().withCommonName("IntermediateCn2").withOrganizationName("MyOrganization").build();
        CERT_ISSUER.generateIntermediateCaCert(intermediateKey1, intermediateCert1, intermediateSubject2, intermediateKey2, intermediateCert2, notBefore, notAfter, 1);

        String caKey = Base64.getEncoder().encodeToString(Files.readAllBytes(rootKey.toPath()));

        // Generate combined Pem files with CA chain in wrong order
        String invalidCombinedPem;
        try (FileInputStream int2CertFis = new FileInputStream(intermediateCert2);
             FileInputStream int1CertFis = new FileInputStream(intermediateCert1);
             FileInputStream rootCertFis = new FileInputStream(rootCert)) {
            String combinedPem = String.join("\n",
                    new String(rootCertFis.readAllBytes(), StandardCharsets.US_ASCII),
                    new String(int1CertFis.readAllBytes(), StandardCharsets.US_ASCII),
                    new String(int2CertFis.readAllBytes(), StandardCharsets.US_ASCII));
            invalidCombinedPem = Base64.getEncoder().encodeToString(combinedPem.getBytes(StandardCharsets.US_ASCII));
        }


        Secret initialClusterCaKeySecret = ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, AbstractModel.clusterCaKeySecretName(NAME), caKey);
        Secret initialClusterCaCertSecret = ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, AbstractModel.clusterCaCertSecretName(NAME), invalidCombinedPem, null, null);

        CustomCaProvider clusterCaProvider = new CustomCaProvider(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, CA_CONFIG, KAFKA, null, null, initialClusterCaCertSecret, initialClusterCaKeySecret);
        Exception clusterCaException = assertThrows(RuntimeException.class, () -> clusterCaProvider.createAndReconcileCa().toCompletableFuture().join());
        assertThat(clusterCaException.getMessage(), is("User supplied Cluster CA cert chain ca.crt is not valid. Certificates must be provided in the correct order."));

        Secret initialClientsCaKeySecret = ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, KafkaResources.clientsCaKeySecretName(NAME), caKey);
        Secret initialClientsCaCertSecret = ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, KafkaResources.clientsCaCertificateSecretName(NAME), invalidCombinedPem, null, null);

        CustomCaProvider clientsCaProvider = new CustomCaProvider(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLIENTS_CA, CA_CONFIG, KAFKA, null, null, initialClientsCaCertSecret, initialClientsCaKeySecret);
        Exception clientsCaException = assertThrows(RuntimeException.class, () -> clientsCaProvider.createAndReconcileCa().toCompletableFuture().join());
        assertThat(clientsCaException.getMessage(), is("User supplied Clients CA cert chain ca.crt is not valid. Certificates must be provided in the correct order."));
    }
}
