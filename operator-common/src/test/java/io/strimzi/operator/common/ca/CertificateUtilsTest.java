/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.ca;

import io.strimzi.certs.OpenSslCertIssuer;
import io.strimzi.certs.StrimziSubject;
import io.strimzi.operator.common.Reconciliation;
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
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CertificateUtilsTest {

    @Test
    @DisplayName("Test cycle of encoding and decoding certificates between PEM and Java X509 implementations")
    public void pemX509Cycle() throws CertificateException {
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

        X509Certificate x509 = CertificateUtils.x509Certificate(cert.getBytes());
        assertThat(x509.getSubjectX500Principal().getName(), is("CN=cluster-ca,O=Default Company Ltd,L=Default City,C=XX"));

        String pem = CertificateUtils.x509CertificateToPem(x509);
        assertThat(pem, is(cert));

        X509Certificate nextX509 = CertificateUtils.x509Certificate(pem.getBytes());
        assertThat(nextX509.getSubjectX500Principal().getName(), is("CN=cluster-ca,O=Default Company Ltd,L=Default City,C=XX"));
        assertThat(nextX509.getSignature(), is(x509.getSignature()));
    }

    @Test
    @DisplayName("When certIsTrusted is called it correctly identifies whether the end entity certificate is issued by the CA cert")
    public void testCertIsTrusted() throws IOException, CertificateException {
        OpenSslCertIssuer ssl = new OpenSslCertIssuer();
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");

        File rootKey = createTempFile("key-", ".key");
        File rootCert = createTempFile("crt-", ".crt");
        File alternateRootKey = createTempFile("key-", ".key");
        File alternateRootCert = createTempFile("crt-", ".crt");
        File key = createTempFile("key-", ".key");
        File csr = createTempFile("csr-", ".csr");
        File cert = createTempFile("crt-", ".crt");

        StrimziSubject rootSubject = new StrimziSubject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

        // Generate a root cert
        Instant now = Instant.now();
        ZonedDateTime notBefore = now.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ZonedDateTime notAfter = now.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1, 4096);

        // Generate alternate root cert
        ssl.generateRootCaCert(rootSubject, alternateRootKey, alternateRootCert, notBefore, notAfter, 1, 4096);

        StrimziSubject subject = new StrimziSubject.Builder()
                .withCommonName("MyCommonName")
                .withOrganizationName("MyOrganization")
                .addDnsName("example1.com")
                .addDnsName("example2.com").build();

        // Generate cert
        ssl.generateCsr(key, csr, subject, 4096);
        ssl.generateCert(csr, rootKey, rootCert, cert, subject, 1);

        X509Certificate x509RootCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(rootCert));
        X509Certificate x509AlternateRootCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(alternateRootCert));
        X509Certificate x509Cert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(cert));

        assertTrue(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509Cert), x509RootCert));
        assertFalse(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509Cert), x509AlternateRootCert));
    }

    @Test
    @DisplayName("When certIsTrusted is called it correctly identifies whether the end entity certificate is valid against a chain with intermediate certificates")
    public void testCertChainWithIntermediateIsTrusted() throws IOException, CertificateException {
        OpenSslCertIssuer ssl = new OpenSslCertIssuer();
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

        StrimziSubject rootSubject = new StrimziSubject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

        // Generate a root cert
        Instant now = Instant.now();
        ZonedDateTime notBefore = now.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ZonedDateTime notAfter = now.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1, 4096);

        // Generate an intermediate cert
        StrimziSubject intermediateSubject1 = new StrimziSubject.Builder().withCommonName("IntermediateCn1").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(rootKey, rootCert, intermediateSubject1, intermediateKey1, intermediateCert1, notBefore, notAfter, 1, 4096);

        // Generate an additional intermediate cert
        StrimziSubject intermediateSubject2 = new StrimziSubject.Builder().withCommonName("IntermediateCn2").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(intermediateKey1, intermediateCert1, intermediateSubject2, intermediateKey2, intermediateCert2, notBefore, notAfter, 1, 4096);


        StrimziSubject subject = new StrimziSubject.Builder()
                .withCommonName("MyCommonName")
                .withOrganizationName("MyOrganization")
                .addDnsName("example1.com")
                .addDnsName("example2.com").build();

        // Generate leaf cert
        ssl.generateCsr(leafKey, csr, subject, 4096);
        ssl.generateCert(csr, intermediateKey2, intermediateCert2, leafCert, subject, 1);

        X509Certificate x509RootCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(rootCert));
        X509Certificate x509IntermediateCert1 = (X509Certificate) certFactory.generateCertificate(new FileInputStream(intermediateCert1));
        X509Certificate x509IntermediateCert2 = (X509Certificate) certFactory.generateCertificate(new FileInputStream(intermediateCert2));
        X509Certificate x509LeafCert = (X509Certificate) certFactory.generateCertificate(new FileInputStream(leafCert));

        assertFalse(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert), x509RootCert));
        assertFalse(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert), x509IntermediateCert1));
        assertTrue(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert), x509IntermediateCert2));

        assertTrue(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert, x509IntermediateCert2), x509IntermediateCert1));
        assertTrue(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert, x509IntermediateCert2, x509IntermediateCert1), x509RootCert));

        assertFalse(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert, x509IntermediateCert1, x509IntermediateCert2), x509RootCert));
        assertFalse(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509LeafCert, x509IntermediateCert2), x509RootCert));
        assertFalse(CertificateUtils.certIsTrusted(Reconciliation.DUMMY_RECONCILIATION, List.of(x509IntermediateCert2, x509IntermediateCert1, x509LeafCert), x509RootCert));
    }

    @Test
    @DisplayName("When the CA data is empty then validateCaCertChain throws an exception")
    public void testValidateCaCertChainWhenEmpty() {
        Exception exception = assertThrows(RuntimeException.class, () -> CertificateUtils.validateCaCertChain(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, Map.of("ca.crt", "")));
        assertEquals("Failed to validate User supplied Cluster CA cert chain in ca.crt", exception.getMessage());
    }

    @Test
    @DisplayName("When the CA data contains a single cert then validateCaCertChain does not throw an exception")
    public void testValidateCaCertChainWhenSingleCert() throws IOException {
        OpenSslCertIssuer ssl = new OpenSslCertIssuer();

        File rootKey = createTempFile("key-", ".key");
        File rootCert = createTempFile("crt-", ".crt");

        StrimziSubject rootSubject = new StrimziSubject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

        // Generate a root cert
        Instant instant = Instant.now();
        ZonedDateTime notBefore = instant.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ZonedDateTime notAfter = instant.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1, 4096);

        Map<String, String> cert;
        try (FileInputStream fis = new FileInputStream(rootCert)) {
            cert = Map.of("ca.crt", Base64.getEncoder().encodeToString(fis.readAllBytes()));
        }

        assertDoesNotThrow(() -> CertificateUtils.validateCaCertChain(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, cert));
    }

    @Test
    @DisplayName("When the CA data contains a chain then validateCaCertChain throws an exception when it is invalid")
    public void testValidateCaCertChain() throws IOException {
        OpenSslCertIssuer ssl = new OpenSslCertIssuer();

        File rootKey = createTempFile("key-", ".key");
        File rootCert = createTempFile("crt-", ".crt");
        File intermediateKey1 = createTempFile("key-", ".key");
        File intermediateCert1 = createTempFile("crt-", ".crt");
        File intermediateKey2 = createTempFile("key-", ".key");
        File intermediateCert2 = createTempFile("crt-", ".crt");

        StrimziSubject rootSubject = new StrimziSubject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();

        // Generate a root cert
        Instant instant = Instant.now();
        ZonedDateTime notBefore = instant.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ZonedDateTime notAfter = instant.plus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
        ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1, 4096);

        // Generate an intermediate cert
        StrimziSubject intermediateSubject1 = new StrimziSubject.Builder().withCommonName("IntermediateCn1").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(rootKey, rootCert, intermediateSubject1, intermediateKey1, intermediateCert1, notBefore, notAfter, 1, 4096);

        // Generate an additional intermediate cert
        StrimziSubject intermediateSubject2 = new StrimziSubject.Builder().withCommonName("IntermediateCn2").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(intermediateKey1, intermediateCert1, intermediateSubject2, intermediateKey2, intermediateCert2, notBefore, notAfter, 1, 4096);

        // Generate another intermediate cert signed directly by the root cert
        File intermediateKey3 = createTempFile("key-", ".key");
        File intermediateCert3 = createTempFile("crt-", ".crt");
        StrimziSubject intermediateSubject3 = new StrimziSubject.Builder().withCommonName("IntermediateCn3").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(rootKey, rootCert, intermediateSubject3, intermediateKey3, intermediateCert3, notBefore, notAfter, 1, 4096);

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

        // Generate combined Pem file with incomplete CA chain (the first intermediate cert is missing)
        String incompleteCombinedPem;
        try (FileInputStream int2CertFis = new FileInputStream(intermediateCert2);
             FileInputStream rootCertFis = new FileInputStream(rootCert)) {
            String combinedPem = String.join("\n",
                    new String(int2CertFis.readAllBytes(), StandardCharsets.US_ASCII),
                    new String(rootCertFis.readAllBytes(), StandardCharsets.US_ASCII));
            incompleteCombinedPem = Base64.getEncoder().encodeToString(combinedPem.getBytes(StandardCharsets.US_ASCII));
        }

        // Generate combined Pem file where the first cert is not issued by the next one (the next one is issued by the root cert)
        String brokenChainCombinedPem;
        try (FileInputStream int3CertFis = new FileInputStream(intermediateCert3);
             FileInputStream int1CertFis = new FileInputStream(intermediateCert1);
             FileInputStream rootCertFis = new FileInputStream(rootCert)) {
            String combinedPem = String.join("\n",
                    new String(int3CertFis.readAllBytes(), StandardCharsets.US_ASCII),
                    new String(int1CertFis.readAllBytes(), StandardCharsets.US_ASCII),
                    new String(rootCertFis.readAllBytes(), StandardCharsets.US_ASCII));
            brokenChainCombinedPem = Base64.getEncoder().encodeToString(combinedPem.getBytes(StandardCharsets.US_ASCII));
        }

        Map<String, String> validCert = Map.of("ca.crt", validCombinedPem, "ca-2026-02-01T09-00-00.crt", validCombinedPem);
        assertDoesNotThrow(() -> CertificateUtils.validateCaCertChain(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, validCert));

        Map<String, String> invalidCert = Map.of("ca.crt", invalidCombinedPem);
        Exception exception = assertThrows(RuntimeException.class, () -> CertificateUtils.validateCaCertChain(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, invalidCert));
        assertThat(exception.getMessage(), startsWith("User supplied Cluster CA cert chain ca.crt is not valid. Certificates must be provided in the correct order and the chain must be complete. Reason: NO_TRUST_ANCHOR"));

        Map<String, String> partiallyValidCert = Map.of("ca.crt", validCombinedPem, "ca-2026-02-01T09-00-00.crt", invalidCombinedPem);
        Exception exception1 = assertThrows(RuntimeException.class, () -> CertificateUtils.validateCaCertChain(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, partiallyValidCert));
        assertThat(exception1.getMessage(), startsWith("User supplied Cluster CA cert chain ca-2026-02-01T09-00-00.crt is not valid. Certificates must be provided in the correct order and the chain must be complete. Reason: NO_TRUST_ANCHOR"));

        Map<String, String> incompleteCert = Map.of("ca.crt", incompleteCombinedPem);
        Exception exception2 = assertThrows(RuntimeException.class, () -> CertificateUtils.validateCaCertChain(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, incompleteCert));
        assertThat(exception2.getMessage(), startsWith("User supplied Cluster CA cert chain ca.crt is not valid. Certificates must be provided in the correct order and the chain must be complete. Reason: NO_TRUST_ANCHOR"));

        Map<String, String> brokenChainCert = Map.of("ca.crt", brokenChainCombinedPem);
        Exception exception3 = assertThrows(RuntimeException.class, () -> CertificateUtils.validateCaCertChain(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, brokenChainCert));
        assertThat(exception3.getMessage(), startsWith("User supplied Cluster CA cert chain ca.crt is not valid. Certificates must be provided in the correct order and the chain must be complete. Reason: NAME_CHAINING"));
    }

    @Test
    @DisplayName("When the CA chain contains an expired intermediate cert then validateCaCertChain reports the expiration as the reason")
    public void testValidateCaCertChainWithExpiredIntermediate() throws IOException {
        Instant now = Instant.now();
        String combinedPem = generateCaChainWithIntermediate("ExpiredIntermediateCn", now.minus(2, ChronoUnit.DAYS), now.minus(1, ChronoUnit.DAYS));

        Map<String, String> expiredCert = Map.of("ca.crt", combinedPem);
        Exception exception = assertThrows(RuntimeException.class, () -> CertificateUtils.validateCaCertChain(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, expiredCert));
        assertThat(exception.getMessage(), startsWith("User supplied Cluster CA cert chain ca.crt is not valid."));
        assertThat(exception.getMessage(), containsString("CN=ExpiredIntermediateCn"));
        assertThat(exception.getMessage(), containsString(" expired on "));
        assertThat(exception.getMessage(), containsString("Reason: EXPIRED"));
        assertThat(exception.getMessage(), not(containsString("Certificates must be provided in the correct order")));
    }

    @Test
    @DisplayName("When the CA chain contains an intermediate cert which is not valid yet then validateCaCertChain reports it as the reason")
    public void testValidateCaCertChainWithNotYetValidIntermediate() throws IOException {
        Instant now = Instant.now();
        String combinedPem = generateCaChainWithIntermediate("NotYetValidIntermediateCn", now.plus(1, ChronoUnit.DAYS), now.plus(2, ChronoUnit.DAYS));

        Map<String, String> notYetValidCert = Map.of("ca.crt", combinedPem);
        Exception exception = assertThrows(RuntimeException.class, () -> CertificateUtils.validateCaCertChain(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, notYetValidCert));
        assertThat(exception.getMessage(), startsWith("User supplied Cluster CA cert chain ca.crt is not valid."));
        assertThat(exception.getMessage(), containsString("CN=NotYetValidIntermediateCn"));
        assertThat(exception.getMessage(), containsString(" is not valid before "));
        assertThat(exception.getMessage(), containsString("Reason: NOT_YET_VALID"));
        assertThat(exception.getMessage(), not(containsString("Certificates must be provided in the correct order")));
    }

    @Test
    @DisplayName("When a cert in the CA chain is not signed by the next cert then validateCaCertChain reports the invalid signature")
    public void testValidateCaCertChainWithInvalidSignature() throws IOException {
        OpenSslCertIssuer ssl = new OpenSslCertIssuer();
        Instant now = Instant.now();
        ZonedDateTime notBefore = toUtcDateTime(now);
        ZonedDateTime notAfter = toUtcDateTime(now.plus(2, ChronoUnit.HOURS));

        File rootKey = createTempFile("key-", ".key");
        File rootCert = createTempFile("crt-", ".crt");
        File intermediateKey = createTempFile("key-", ".key");
        File intermediateCert = createTempFile("crt-", ".crt");
        File renewedIntermediateKey = createTempFile("key-", ".key");
        File renewedIntermediateCert = createTempFile("crt-", ".crt");
        File childKey = createTempFile("key-", ".key");
        File childCert = createTempFile("crt-", ".crt");

        StrimziSubject rootSubject = new StrimziSubject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();
        ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1, 2048);

        // Two intermediate certs with the same subject but with different keys (e.g. an intermediate CA renewed with a new key)
        StrimziSubject intermediateSubject = new StrimziSubject.Builder().withCommonName("IntermediateCn").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(rootKey, rootCert, intermediateSubject, intermediateKey, intermediateCert, notBefore, notAfter, 0, 2048);
        ssl.generateIntermediateCaCert(rootKey, rootCert, intermediateSubject, renewedIntermediateKey, renewedIntermediateCert, notBefore, notAfter, 0, 2048);

        // The child cert is signed by the key of the original intermediate cert
        StrimziSubject childSubject = new StrimziSubject.Builder().withCommonName("ChildCn").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(intermediateKey, intermediateCert, childSubject, childKey, childCert, notBefore, notAfter, 0, 2048);

        // The chain contains the renewed intermediate cert, which has the right subject but a different key
        Map<String, String> invalidSignatureCert = Map.of("ca.crt", combineToBase64Pem(childCert, renewedIntermediateCert, rootCert));
        Exception exception = assertThrows(RuntimeException.class, () -> CertificateUtils.validateCaCertChain(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, invalidSignatureCert));
        assertThat(exception.getMessage(), startsWith("User supplied Cluster CA cert chain ca.crt is not valid."));
        assertThat(exception.getMessage(), containsString("CN=ChildCn"));
        assertThat(exception.getMessage(), containsString(" is not signed by the next certificate in the chain."));
        assertThat(exception.getMessage(), containsString("Reason: INVALID_SIGNATURE"));
    }

    @Test
    @DisplayName("When a cert in the middle of the CA chain is not a CA cert then validateCaCertChain reports that it cannot sign other certs")
    public void testValidateCaCertChainWithNonCaIntermediate() throws IOException {
        OpenSslCertIssuer ssl = new OpenSslCertIssuer();
        Instant now = Instant.now();
        ZonedDateTime notBefore = toUtcDateTime(now);
        ZonedDateTime notAfter = toUtcDateTime(now.plus(2, ChronoUnit.HOURS));

        File rootKey = createTempFile("key-", ".key");
        File rootCert = createTempFile("crt-", ".crt");
        File leafKey = createTempFile("key-", ".key");
        File leafCsr = createTempFile("csr-", ".csr");
        File leafCert = createTempFile("crt-", ".crt");
        File childKey = createTempFile("key-", ".key");
        File childCert = createTempFile("crt-", ".crt");

        StrimziSubject rootSubject = new StrimziSubject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();
        ssl.generateRootCaCert(rootSubject, rootKey, rootCert, notBefore, notAfter, 1, 2048);

        // A leaf cert (not a CA cert) signed by the root cert
        StrimziSubject leafSubject = new StrimziSubject.Builder().withCommonName("LeafCn").withOrganizationName("MyOrganization").build();
        ssl.generateCsr(leafKey, leafCsr, leafSubject, 2048);
        ssl.generateCert(leafCsr, rootKey, rootCert, leafCert, leafSubject, notBefore, notAfter);

        // A cert signed by the leaf cert
        StrimziSubject childSubject = new StrimziSubject.Builder().withCommonName("ChildCn").withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(leafKey, leafCert, childSubject, childKey, childCert, notBefore, notAfter, 0, 2048);

        Map<String, String> nonCaCert = Map.of("ca.crt", combineToBase64Pem(childCert, leafCert, rootCert));
        Exception exception = assertThrows(RuntimeException.class, () -> CertificateUtils.validateCaCertChain(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, nonCaCert));
        assertThat(exception.getMessage(), startsWith("User supplied Cluster CA cert chain ca.crt is not valid."));
        assertThat(exception.getMessage(), containsString("CN=LeafCn"));
        // Depending on the order of the checks, the JDK reports NOT_CA_CERT or INVALID_KEY_USAGE. Both result in the same message.
        assertThat(exception.getMessage(), containsString(" is not allowed to sign other certificates. It must be a CA certificate with the keyCertSign key usage."));
    }

    /**
     * Combines the given PEM files into a single Base64 encoded PEM.
     *
     * @param certs     PEM files with the certificates, in the order in which they should be combined
     *
     * @return  Base64 encoded PEM with all the certificates
     */
    private String combineToBase64Pem(File... certs) throws IOException {
        StringBuilder pem = new StringBuilder();

        for (File cert : certs) {
            try (FileInputStream fis = new FileInputStream(cert)) {
                pem.append(new String(fis.readAllBytes(), StandardCharsets.US_ASCII)).append("\n");
            }
        }

        return Base64.getEncoder().encodeToString(pem.toString().getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Generates a Base64 encoded CA chain in the correct order, with a valid root cert and an intermediate cert
     * with the given validity period.
     *
     * @param intermediateCn    Common name of the intermediate cert
     * @param notBefore         Start of the validity period of the intermediate cert
     * @param notAfter          End of the validity period of the intermediate cert
     *
     * @return  Base64 encoded PEM with the intermediate cert followed by the root cert
     */
    private String generateCaChainWithIntermediate(String intermediateCn, Instant notBefore, Instant notAfter) throws IOException {
        OpenSslCertIssuer ssl = new OpenSslCertIssuer();

        File rootKey = createTempFile("key-", ".key");
        File rootCert = createTempFile("crt-", ".crt");
        File intermediateKey = createTempFile("key-", ".key");
        File intermediateCert = createTempFile("crt-", ".crt");

        // Generate a valid root cert
        Instant now = Instant.now();
        StrimziSubject rootSubject = new StrimziSubject.Builder().withCommonName("RootCn").withOrganizationName("MyOrganization").build();
        ssl.generateRootCaCert(rootSubject, rootKey, rootCert, toUtcDateTime(now), toUtcDateTime(now.plus(2, ChronoUnit.HOURS)), 1, 2048);

        // Generate the intermediate cert with the given validity period
        StrimziSubject intermediateSubject = new StrimziSubject.Builder().withCommonName(intermediateCn).withOrganizationName("MyOrganization").build();
        ssl.generateIntermediateCaCert(rootKey, rootCert, intermediateSubject, intermediateKey, intermediateCert, toUtcDateTime(notBefore), toUtcDateTime(notAfter), 0, 2048);

        // Generate combined Pem file with the CA chain in the correct order
        try (FileInputStream intCertFis = new FileInputStream(intermediateCert);
             FileInputStream rootCertFis = new FileInputStream(rootCert)) {
            String pem = String.join("\n",
                    new String(intCertFis.readAllBytes(), StandardCharsets.US_ASCII),
                    new String(rootCertFis.readAllBytes(), StandardCharsets.US_ASCII));
            return Base64.getEncoder().encodeToString(pem.getBytes(StandardCharsets.US_ASCII));
        }
    }

    private static ZonedDateTime toUtcDateTime(Instant instant) {
        return instant.truncatedTo(ChronoUnit.SECONDS).atZone(Clock.systemUTC().getZone());
    }

    private File createTempFile(String prefix, String suffix) throws IOException {
        File file = File.createTempFile(prefix, suffix);
        file.deleteOnExit();
        return file;
    }
}