/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CaTest {

    /**
     * Mock implementation of the CA class which does not generate the CA certificate
     */
    private static class MockCa extends Ca {

        /**
         * Constructs the CA object
         *
         * @param reconciliation    Reconciliation marker
         * @param certManager       Certificate manager instance
         * @param passwordGenerator Password generator instance
         * @param caCertSecret      Kubernetes Secret where the CA public key will be stored
         * @param caKeySecret       Kubernetes Secret where the CA private key will be stored
         */
        public MockCa(Reconciliation reconciliation, CertManager certManager, PasswordGenerator passwordGenerator, Secret caCertSecret, Secret caKeySecret, boolean generateCa) {
            super(reconciliation, certManager, passwordGenerator, "mock", "mock-ca-secret", caCertSecret, "mock-key-secret", caKeySecret, CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS, CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS, generateCa, null);
        }

        @Override
        protected String caCertGenerationAnnotation() {
            return "mock";
        }

        @Override
        protected String caName() {
            return "Mock CA";
        }
    }

    private Ca ca;
    private Duration oneYear;
    private Clock now;

    @BeforeEach
    public void setup() {
        now = Clock.fixed(new Date().toInstant(), Clock.systemUTC().getZone());
        oneYear = Duration.ofDays(CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS);
        ca = new MockCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(now), new PasswordGenerator(10, "a", "a"), null, null, true);
    }

    @Test
    @DisplayName("Should return certificate expiration date as epoch when certificate is present")
    void shouldReturnCertificateExpirationDateEpoch() {
        ca.createRenewOrReplace(true, false, false);

        Instant inOneYear = Clock.offset(now, oneYear).instant();
        long expectedEpoch = inOneYear.truncatedTo(ChronoUnit.SECONDS).toEpochMilli();
        long actualEpoch = ca.getCertificateExpirationDateEpoch();
        assertEquals(expectedEpoch, actualEpoch, "Expected and actual certificate expiration epochs should match");
    }

    @Test
    @DisplayName("Should raise RuntimeException when certificate is not present")
    void shouldReturnZeroWhenCertificateNotPresent() {
        Exception exception = assertThrows(RuntimeException.class, () -> ca.getCertificateExpirationDateEpoch());
        assertEquals("ca.crt does not exist in the secret mock-ca-secret", exception.getMessage());
    }

    @Test
    @DisplayName("Test cycle of encoding and decoding certificates between PEM and Java X509 implementations")
    void pemX509Cycle() throws CertificateException {
        // This certificate is used for testing purposes only and is not a real certificate. It is valid until 2118,
        // so it should not cause any issues with the tests.
        String cert = """
            -----BEGIN CERTIFICATE-----
            MIIDhjCCAm6gAwIBAgIJANzx2pPcYgmlMA0GCSqGSIb3DQEBCwUAMFcxCzAJBgNV
            BAYTAlhYMRUwEwYDVQQHDAxEZWZhdWx0IENpdHkxHDAaBgNVBAoME0RlZmF1bHQg
            Q29tcGFueSBMdGQxEzARBgNVBAMMCmNsdXN0ZXItY2EwIBcNMTgwODIzMTYxOTU0
            WhgPMjExODA3MzAxNjE5NTRaMFcxCzAJBgNVBAYTAlhYMRUwEwYDVQQHDAxEZWZh
            dWx0IENpdHkxHDAaBgNVBAoME0RlZmF1bHQgQ29tcGFueSBMdGQxEzARBgNVBAMM
            CmNsdXN0ZXItY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDbFnJj
            90sKoM35VszJsfwNvO5dshoeFIb2idf7h+l0h3GMv29j+1XtmLJGzxiYy320KFZr
            3IKWbq+DabqdlqEqZm9NZ1Kq9d7mB10zulQce5JwVZ3FqpCmLku2jHCaDXzTKC3T
            /Xp0O9Oe8+42ysSMCTd8p8aZ4vAyJMCKcoyVCGHrUWVba40D7cQNOlhJplSzHZdL
            FYZ13kwzpT5GpDEPhGVmtF8qV918lSxvdpuepyeFdOSYY88FEMMLLrlZG4QCPyES
            4FpcUXMzzvZeLIlZnKNIYbao3Kx+yZv//wjC80/pqdyoZ5+K5hDxjby2+f+2dh0T
            adKRZC2pp+j3/z63AgMBAAGjUzBRMB0GA1UdDgQWBBThuvddCb/5TPSKYNOHkCTL
            VghhRzAfBgNVHSMEGDAWgBThuvddCb/5TPSKYNOHkCTLVghhRzAPBgNVHRMBAf8E
            BTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBA6oTI27dJgbVtyWxQWznKrkznZ9+t
            mQQGbpfl9zEg7/0X7fFb+m84QHro+aNnQ4kTgZ6QBvusIpwfx1F6lQrraVrPr142
            4DqGmY9xReNu/fj+C+8lTI5PA+mE7tMrLpQvKxI+AMttvlz8eo1SITUA+kJEiWZX
            mjvyHXmhic4K8SnnB0gnFzHN4y09wLqRMNCRH+aI+sa9Wu8cqvpTqlelVcYV83zu
            ydx4VZkC+zTzjI418znN/NU2CMpxLZNl0/zCrspID7v34NRmJ1AHFcrn7/XhsSvz
            D0z+vgrfionoRhyWUDh7POlWwdUOWiBDBOFrkgeKNphSC0glYFN+2IW7
            -----END CERTIFICATE-----""";

        X509Certificate x509 = Ca.x509Certificate(cert.getBytes());
        assertThat(x509.getSubjectX500Principal().getName(), is("CN=cluster-ca,O=Default Company Ltd,L=Default City,C=XX"));

        String pem = Ca.x509CertificateToPem(x509);
        assertThat(pem, is(cert));

        X509Certificate nextX509 = Ca.x509Certificate(pem.getBytes());
        assertThat(nextX509.getSubjectX500Principal().getName(), is("CN=cluster-ca,O=Default Company Ltd,L=Default City,C=XX"));
        assertThat(nextX509.getSignature(), is(x509.getSignature()));
    }

    @Test
    @DisplayName("When certIsTrusted is called it correctly identifies whether the end entity certificate is issued by the CA cert")
    public void testCertIsTrusted() throws IOException, CertificateException {
        OpenSslCertManager ssl = new OpenSslCertManager();
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");

        File rootKey = createTempFile("key-", ".key");
        File rootCert = createTempFile("crt-", ".crt");
        File alternateRootKey = createTempFile("key-", ".key");
        File alternateRootCert = createTempFile("crt-", ".crt");
        File key = createTempFile("key-", ".key");
        File csr = createTempFile("csr-", ".csr");
        File cert = createTempFile("crt-", ".crt");

        Subject rootSubject = new Subject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

        // Generate a root cert
        Instant now = Instant.now();
        ZonedDateTime notBefore = now.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ZonedDateTime notAfter = now.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1);

        // Generate alternate root cert
        ssl.generateRootCaCert(rootSubject, alternateRootKey, alternateRootCert, notBefore, notAfter, 1);

        Subject subject = new Subject.Builder()
                .withCommonName("MyCommonName")
                .withOrganizationName("MyOrganization")
                .addDnsName("example1.com")
                .addDnsName("example2.com").build();

        // Generate cert
        ssl.generateCsr(key, csr, subject);
        ssl.generateCert(csr, rootKey, rootCert, cert, subject, 1);

        X509Certificate x509RootCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(rootCert));
        X509Certificate x509AlternateRootCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(alternateRootCert));
        X509Certificate x509Cert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(cert));

        assertTrue(Ca.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509Cert), x509RootCert));
        assertFalse(Ca.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509Cert), x509AlternateRootCert));
    }

    @Test
    @DisplayName("When certIsTrusted is called it correctly identifies whether the end entity certificate is valid against a chain with intermediate certificates")
    public void testCertChainWithIntermediateIsTrusted() throws IOException, CertificateException {
        OpenSslCertManager ssl = new OpenSslCertManager();
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");

        File rootKey = createTempFile("key-", ".key");
        File rootCert = createTempFile("crt-", ".crt");
        File intermediateKey1 = createTempFile("key-", ".key");
        File intermediateCert1 = createTempFile("crt-", ".crt");
        File intermediateKey2 = createTempFile("key-", ".key");
        File intermediateCert2 = createTempFile("crt-", ".crt");
        File leafKey = createTempFile("key-", ".key");
        File csr = createTempFile("csr-", ".csr");
        File leafCert = createTempFile("crt-", ".crt");

        Subject rootSubject = new Subject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

        // Generate a root cert
        Instant now = Instant.now();
        ZonedDateTime notBefore = now.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ZonedDateTime notAfter = now.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1);

        // Generate an intermediate cert
        Subject intermediateSubject1 = new Subject.Builder().withCommonName("IntermediateCn1").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(rootKey, rootCert, intermediateSubject1, intermediateKey1, intermediateCert1, notBefore, notAfter, 1);

        // Generate an additional intermediate cert
        Subject intermediateSubject2 = new Subject.Builder().withCommonName("IntermediateCn2").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(intermediateKey1, intermediateCert1, intermediateSubject2, intermediateKey2, intermediateCert2, notBefore, notAfter, 1);


        Subject subject = new Subject.Builder()
                .withCommonName("MyCommonName")
                .withOrganizationName("MyOrganization")
                .addDnsName("example1.com")
                .addDnsName("example2.com").build();

        // Generate leaf cert
        ssl.generateCsr(leafKey, csr, subject);
        ssl.generateCert(csr, intermediateKey2, intermediateCert2, leafCert, subject, 1);

        X509Certificate x509RootCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(rootCert));
        X509Certificate x509IntermediateCert1 = (X509Certificate) certFactory.generateCertificate(new FileInputStream(intermediateCert1));
        X509Certificate x509IntermediateCert2 = (X509Certificate) certFactory.generateCertificate(new FileInputStream(intermediateCert2));
        X509Certificate x509LeafCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(leafCert));

        assertFalse(Ca.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert), x509RootCert));
        assertFalse(Ca.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert), x509IntermediateCert1));
        assertTrue(Ca.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert), x509IntermediateCert2));

        assertTrue(Ca.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert, x509IntermediateCert2), x509IntermediateCert1));
        assertTrue(Ca.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert, x509IntermediateCert2, x509IntermediateCert1), x509RootCert));

        assertFalse(Ca.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert, x509IntermediateCert1, x509IntermediateCert2), x509RootCert));
        assertFalse(Ca.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert, x509IntermediateCert2), x509RootCert));
        assertFalse(Ca.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509IntermediateCert2, x509IntermediateCert1, x509LeafCert), x509RootCert));
    }

    @Test
    @DisplayName("When the CA data is empty then validateUserCaCertChain throws an exception")
    public void testValidateUserCaCertChainWhenEmpty() {
        Secret certSecret = new SecretBuilder()
                .withNewMetadata()
                .endMetadata()
                .withData(Map.of("ca.crt", ""))
                .build();
        Secret keySecret = new SecretBuilder()
                .withNewMetadata()
                .endMetadata()
                .withData(Map.of("ca.key", "ca-key"))
                .build();
        Exception exception = assertThrows(RuntimeException.class, () -> new MockCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(now), new PasswordGenerator(10, "a", "a"), certSecret, keySecret, false));
        assertEquals("Failed to validate User supplied Mock CA cert chain in ca.crt", exception.getMessage());
    }

    @Test
    @DisplayName("When the CA data contains a single cert then validateUserCaCertChain does not throw an exception")
    public void testValidateUserCaCertChainWhenSingleCert() throws IOException {
        OpenSslCertManager ssl = new OpenSslCertManager();

        File rootKey = createTempFile("key-", ".key");
        File rootCert = createTempFile("crt-", ".crt");

        Subject rootSubject = new Subject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

        // Generate a root cert
        Instant instant = Instant.now();
        ZonedDateTime notBefore = instant.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ZonedDateTime notAfter = instant.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1);

        Secret keySecret = new SecretBuilder()
                .withNewMetadata()
                .endMetadata()
                .withData(Map.of("ca.key", "ca-key"))
                .build();

        Secret certSecret;
        try (FileInputStream fis = new FileInputStream(rootCert)) {
            certSecret = new SecretBuilder()
                    .withNewMetadata()
                    .endMetadata()
                    .withData(Map.of("ca.crt", Base64.getEncoder().encodeToString(fis.readAllBytes())))
                    .build();
        }

        assertDoesNotThrow(() -> new MockCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(now), new PasswordGenerator(10, "a", "a"), certSecret, keySecret, false));
    }

    @Test
    @DisplayName("When the CA data contains a chain then validateUserCaCertChain throws an exception when it is invalid")
    public void testValidateUserCaCertChain() throws IOException {
        OpenSslCertManager ssl = new OpenSslCertManager();

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

        Secret keySecret = new SecretBuilder()
                .withNewMetadata()
                .endMetadata()
                .withData(Map.of("ca.key", "ca-key"))
                .build();

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

        Secret validCertSecret = new SecretBuilder()
                .withNewMetadata()
                .endMetadata()
                .withData(Map.of("ca.crt", validCombinedPem,
                        "ca-2026-02-01T09-00-00.crt", validCombinedPem))
                .build();
        assertDoesNotThrow(() -> new MockCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(now), new PasswordGenerator(10, "a", "a"), validCertSecret, keySecret, false));

        Secret invalidCertSecret = new SecretBuilder()
                .withNewMetadata()
                .endMetadata()
                .withData(Map.of("ca.crt", invalidCombinedPem))
                .build();
        Exception exception = assertThrows(RuntimeException.class, () -> new MockCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(now), new PasswordGenerator(10, "a", "a"), invalidCertSecret, keySecret, false));
        assertEquals("User supplied Mock CA cert chain ca.crt is not valid. Certificates must be provided in the correct order.", exception.getMessage());

        Secret partiallyValidCertSecret = new SecretBuilder()
                .withNewMetadata()
                .endMetadata()
                .withData(Map.of("ca.crt", validCombinedPem,
                        "ca-2026-02-01T09-00-00.crt", invalidCombinedPem))
                .build();
        Exception exception1 = assertThrows(RuntimeException.class, () -> new MockCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(now), new PasswordGenerator(10, "a", "a"), partiallyValidCertSecret, keySecret, false));
        assertEquals("User supplied Mock CA cert chain ca-2026-02-01T09-00-00.crt is not valid. Certificates must be provided in the correct order.", exception1.getMessage());
    }

    private File createTempFile(String prefix, String suffix) throws IOException {
        File file = File.createTempFile(prefix, suffix);
        file.deleteOnExit();
        return file;
    }
}