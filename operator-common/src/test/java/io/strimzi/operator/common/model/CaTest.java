/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        public MockCa(Reconciliation reconciliation, CertManager certManager, PasswordGenerator passwordGenerator, Secret caCertSecret, Secret caKeySecret) {
            super(reconciliation, certManager, passwordGenerator, "mock", "mock-ca-secret", caCertSecret, "mock-key-secret", caKeySecret, CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS, CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS, true, null);
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
        ca = new MockCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(now), new PasswordGenerator(10, "a", "a"), null, null);
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
}