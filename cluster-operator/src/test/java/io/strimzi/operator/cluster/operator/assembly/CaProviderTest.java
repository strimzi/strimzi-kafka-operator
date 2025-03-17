/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.certs.OpenSslCertIssuer;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CaProviderTest {

    static class DummyCaProvider extends CaProvider {

        /**
         * Constructor.
         */
        public DummyCaProvider() {
            super(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, CaConfig.createDefault(), new KafkaBuilder().build(), null, null);
        }

        @Override
        public CompletionStage<Ca> createCa() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Secret> reconcileCaSecrets() {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Test
    @DisplayName("When the CA data is empty then validateUserCaCertChain throws an exception")
    public void testValidateUserCaCertChainWhenEmpty() {
        Exception exception = assertThrows(RuntimeException.class, () -> new DummyCaProvider().validateUserCaCertChain(Map.of("ca.crt", "")));
        assertEquals("Failed to validate User supplied Cluster CA cert chain in ca.crt", exception.getMessage());
    }

    @Test
    @DisplayName("When the CA data contains a single cert then validateUserCaCertChain does not throw an exception")
    public void testValidateUserCaCertChainWhenSingleCert() throws IOException {
        OpenSslCertIssuer ssl = new OpenSslCertIssuer();

        File rootKey = createTempFile("key-", ".key");
        File rootCert = createTempFile("crt-", ".crt");

        Subject rootSubject = new Subject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

        // Generate a root cert
        Instant instant = Instant.now();
        ZonedDateTime notBefore = instant.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ZonedDateTime notAfter = instant.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1);

        Map<String, String> cert;
        try (FileInputStream fis = new FileInputStream(rootCert)) {
            cert = Map.of("ca.crt", Base64.getEncoder().encodeToString(fis.readAllBytes()));
        }

        assertDoesNotThrow(() -> new DummyCaProvider().validateUserCaCertChain(cert));
    }

    @Test
    @DisplayName("When the CA data contains a chain then validateUserCaCertChain throws an exception when it is invalid")
    public void testValidateUserCaCertChain() throws IOException {
        OpenSslCertIssuer ssl = new OpenSslCertIssuer();

        File rootKey = createTempFile("key-", ".key");
        File rootCert = createTempFile("crt-", ".crt");
        File intermediateKey1 = createTempFile("key-", ".key");
        File intermediateCert1 = createTempFile("crt-", ".crt");
        File intermediateKey2 = createTempFile("key-", ".key");
        File intermediateCert2 = createTempFile("crt-", ".crt");

        Subject rootSubject = new Subject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

        // Generate a root cert
        Instant instant = Instant.now();
        ZonedDateTime notBefore = instant.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ZonedDateTime notAfter = instant.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1);

        // Generate an intermediate cert
        Subject intermediateSubject1 = new Subject.Builder().withCommonName("IntermediateCn1").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(rootKey, rootCert, intermediateSubject1, intermediateKey1, intermediateCert1, notBefore, notAfter, 1);

        // Generate an additional intermediate cert
        Subject intermediateSubject2 = new Subject.Builder().withCommonName("IntermediateCn2").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(intermediateKey1, intermediateCert1, intermediateSubject2, intermediateKey2, intermediateCert2, notBefore, notAfter, 1);

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

        Map<String, String> validCert = Map.of("ca.crt", validCombinedPem, "ca-2026-02-01T09-00-00.crt", validCombinedPem);
        assertDoesNotThrow(() -> new DummyCaProvider().validateUserCaCertChain(validCert));

        Map<String, String> invalidCert = Map.of("ca.crt", invalidCombinedPem);
        Exception exception = assertThrows(RuntimeException.class, () -> new DummyCaProvider().validateUserCaCertChain(invalidCert));
        assertEquals("User supplied Cluster CA cert chain ca.crt is not valid. Certificates must be provided in the correct order.", exception.getMessage());

        Map<String, String> partiallyValidCert = Map.of("ca.crt", validCombinedPem, "ca-2026-02-01T09-00-00.crt", invalidCombinedPem);
        Exception exception1 = assertThrows(RuntimeException.class, () -> new DummyCaProvider().validateUserCaCertChain(partiallyValidCert));
        assertEquals("User supplied Cluster CA cert chain ca-2026-02-01T09-00-00.crt is not valid. Certificates must be provided in the correct order.", exception1.getMessage());
    }

    private File createTempFile(String prefix, String suffix) throws IOException {
        File file = File.createTempFile(prefix, suffix);
        file.deleteOnExit();
        return file;
    }
}
