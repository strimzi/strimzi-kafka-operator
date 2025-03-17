/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.ca;

import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class InternalCaCertIssuerTest {
    private static final Subject SUBJECT = new Subject.Builder().build();
    private static final String POD = "pod0";

    // This certificate is used for testing purposes only. It is valid until 2119, so it should not cause any issues with the tests.
    private final static String DUMMY_CERT = "-----BEGIN CERTIFICATE-----\n" +
            "MIIDLDCCAhSgAwIBAgIUAw8AFcPvJkD5ijYTuT5KBt6sUX8wDQYJKoZIhvcNAQEL\n" +
            "BQAwLTETMBEGA1UECgwKaW8uc3RyaW16aTEWMBQGA1UEAwwNY2x1c3Rlci1jYSB2\n" +
            "MDAgFw0yNjA2MzAxMjU0MTNaGA8yMTE5MDYwODEyNTQxM1owFTETMBEGA1UEAwwK\n" +
            "dmFsaWQtdXNlcjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAJvaDeKe\n" +
            "8XQgS8bwp8XTjnaO07+sVufu9vg/2HuDExn5oG2OFYSIZzS6Sl03xrsoQXBw8LLy\n" +
            "XExmem+iZM1r5zVAvqrmD4GIdc+SRalpGdA1STm3PUUUiAVAaM2wlrXMQm8xSIjL\n" +
            "tMUcLSsg8zzbCjwiTQ4F8svvFGBSEkLfuxYzonQlp6pfRhS66Hl1GAY7lOBlXmwy\n" +
            "5rZbJkJMy+vVt/6uQe4gJonRmOPqofeGEVPmfyPCNgl1cIETMxv1JJS74hFM3pSf\n" +
            "w1zl4WEAuVNA04tIMrFCQC8vMmxs0/spMzNt9adVnHDWE0/FwExYWKPNFYXPt3nW\n" +
            "O3nljkEYNlFcUhcCAwEAAaNaMFgwCQYDVR0TBAIwADALBgNVHQ8EBAMCBaAwHQYD\n" +
            "VR0OBBYEFPhOVigIhxQpcTlsrM68V9M7k2IaMB8GA1UdIwQYMBaAFJXl3KBqS4/x\n" +
            "rjOAskayMxAzPjWQMA0GCSqGSIb3DQEBCwUAA4IBAQAZqNkA/cl0AWKhbYLPiOVM\n" +
            "cVbjO5cKeaiKOswLyUlRDUElBPv0+zV2Q7x1JckxP/5nKo1CIQMHaWuYnrdnEBZc\n" +
            "6xmS/DCtgD7+FWUpbMdD4cL+iFYkH5DsnG0D5AYo73/wbg/+e+38YPdYUYz94rG/\n" +
            "1YfY/7+U2lXpncPkqIMwDiJBBKvCVCEyY/KOqbq4urEYUVHXNlZ1YcSOTqHYyY2C\n" +
            "WpP//JDkel5JYikIMqp5BE9HKWBkZVkSS9kisHJkskbcKcCW1jCWwHE2hRqWE0WZ\n" +
            "91mD7YKS3VB3cQ/mBqYpf2bCf1zQZtqOB7dfbRevHMYlfKyH+qX7rlfujF8nOP/g\n" +
            "-----END CERTIFICATE-----\n";


    // Certificate used for expiration tests where actual expiration is needed. This certificate expires on 27th March 2023.
    // But with correct configuration or renewal days before expiration, it can be used to trigger expiration,
    private final static String EXPIRED_DUMMY_CERT = "-----BEGIN CERTIFICATE-----\n" +
            "MIIECTCCAfGgAwIBAgIUAw8AFcPvJkD5ijYTuT5KBt6sUX4wDQYJKoZIhvcNAQEN\n" +
            "BQAwLTETMBEGA1UECgwKaW8uc3RyaW16aTEWMBQGA1UEAwwNY2xpZW50cy1jYSB2\n" +
            "MDAeFw0yMjAzMjcxNTQyNTBaFw0yMzAzMjcxNTQyNTBaMA8xDTALBgNVBAMMBHVz\n" +
            "ZXIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC8cpNdaHYyZuPJ2p1I\n" +
            "2LpEN5nwrE6Bys79ITbfwj+C12O5wyLp+n0VNr/7DPZUQP71vwWDdSmrP2gW6OSV\n" +
            "EOb40mjLvRSRRDrcowNXL6NlV+tzd7QuNgBilWssBfpvBGYHsux7dA7Qf+DGv/Wp\n" +
            "Wqw31ybPk5NqQXzRjJ+6xVLjERlLuIGy0s4XsO4Grfuwa1Le40KVoHNR+BRft+H6\n" +
            "wajKnUP/j0hJHOgYmYNeuB6Aw8T49HctKJzay/b/0Jud1Vre3/cS4l5EyKpq1H3l\n" +
            "DWPShSY0CdcvmVkSoqRJeRbqeRrUrAdzWtOIXVBuI9SonAov5RHBtaX+rZldALZD\n" +
            "o3FrAgMBAAGjPzA9MB0GA1UdDgQWBBTO8o3wkH+x7WSJNO9Gi316f5SBADAMBgNV\n" +
            "HRMBAf8EAjAAMA4GA1UdDwEB/wQEAwIFoDANBgkqhkiG9w0BAQ0FAAOCAgEAjGBr\n" +
            "wBlL2Bxcqo8BbRsLtQyRjiOtG+K0gniMAJaX5T6zUxPouzw4fkz+FMlyU+/SGYHt\n" +
            "wDKhe6pNqls5If884i2R/Vkl4PpX1WMi1BewzdENIGkQFKzjRQd/yCKqlW2+QaNM\n" +
            "H+u+K5l6yxyZ0FWH5pf7XVMpgs8MI/0Hq1349Lh56Ekcna8MZNxg1wBjMQzSrv9U\n" +
            "QUV7ITOCt4ghYsUx3gaoehLt9lXnnNWCW7o/7VcZEfxV1Cr6pm+cgfvqS+sTeb8E\n" +
            "xxlIVuwhVuT6kxzepjEceXrletj66aITAZlg3xPQwzw3jYX354HXkmpDox2KvcLK\n" +
            "xKhBfbqbEZbI/kVKZF6XQEWmflnz/kiy1hTfHBNRuOTu/pHb4LHXW0b4qUcljxRR\n" +
            "412HUn/OTulvqtU9pQi442+KzxFX+bm6mQwO95eZbte8omK5EzWZkvop6CjT4V9a\n" +
            "Rnb2PLgqNCSBkp4XhR77bdWI8y569y+lcMckj6xK2ct1OGNpudClkd0oeUb/MZnT\n" +
            "4ebFTZY24EM5LNmWXaR6RVmbg0Xc1kSR8DqUzTaNA2s8lbtQId4yvzxOP5Lkcq/G\n" +
            "dJl3QtzbWBWFW2bU8MHZ2bUQsmw0RtmTg9tDMCHLAH+9Mw7yMWsEg5iX0H7hnwJA\n" +
            "T/DiI+A2t2dGukf5qfzqgiXkq4XqM6+p0zY1Cv0=\n" +
            "-----END CERTIFICATE-----\n";

    @Test
    public void renewalOfCertificatesWithNullCertificates() throws IOException {
        InternalCa mockedCa = new MockedClusterCa();

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                POD,
                SUBJECT,
                null,
                true,
                false
        ).toCompletableFuture().join();

        assertThat(new String(newCert.cert()), is("new-cert0"));
        assertThat(new String(newCert.key()), is("new-key0"));
        assertThat(new String(newCert.keyStore()), is("new-keystore0"));
        assertThat(newCert.storePassword(), is("new-password0"));
    }

    @Test
    public void renewalOfCertificatesWithCaRenewal() {
        MockedClusterCa mockedCa = new MockedClusterCa();
        mockedCa.setCaCertGeneration(1);

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8));
        CertAndKey newCert = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                POD,
                SUBJECT,
                initialCert,
                true,
                false
        ).toCompletableFuture().join();

        assertThat(new String(newCert.cert()), is("new-cert0"));
        assertThat(new String(newCert.key()), is("new-key0"));
        assertThat(new String(newCert.keyStore()), is("new-keystore0"));
        assertThat(newCert.storePassword(), is("new-password0"));
        assertThat(newCert.caCertGeneration(), is(1));
    }

    @Test
    public void renewalOfCertificatesDelayedRenewalInWindow() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8));

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                POD,
                SUBJECT,
                initialCert,
                true,
                false
        ).toCompletableFuture().join();

        assertThat(new String(newCert.cert()), is("new-cert0"));
        assertThat(new String(newCert.key()), is("new-key0"));
        assertThat(new String(newCert.keyStore()), is("new-keystore0"));
        assertThat(newCert.storePassword(), is("new-password0"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    @Test
    public void renewalOfCertificatesDelayedRenewalOutsideWindow() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8));

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                POD,
                SUBJECT,
                initialCert,
                false,
                false
        ).toCompletableFuture().join();

        assertThat(new String(newCert.cert()), is(EXPIRED_DUMMY_CERT));
        assertThat(new String(newCert.key()), is("old-key"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    @Test
    public void noRenewalOfCertificates() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8));

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                POD,
                SUBJECT,
                initialCert,
                true,
                false
        ).toCompletableFuture().join();

        assertThat(new String(newCert.cert()), is(DUMMY_CERT));
        assertThat(new String(newCert.key()), is("old-key"));
    }

    @Test
    public void changedSubjectOfCertificates() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8));

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                POD,
                 new Subject.Builder().addDnsName("pod0.test.com").build(),
                initialCert,
                true,
                false
        ).toCompletableFuture().join();

        assertThat(new String(newCert.cert()), is("new-cert0"));
        assertThat(new String(newCert.key()), is("new-key0"));
    }


    @Test
    public void certificatesIncludeCaChain() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                POD,
                SUBJECT,
                null,
                true,
                true
        ).toCompletableFuture().join();

        assertThat(new String(newCert.cert()), is("new-cert0CA-CERT"));
    }

    @Test
    public void caChainAddedToExistingCertificates() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey initialCert = new CertAndKey("new-key0".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8));

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                "pod0",
                SUBJECT,
                initialCert,
                true,
                true
        ).toCompletableFuture().join();

        assertThat(new String(newCert.cert()), is("new-cert0CA-CERT"));
    }

    @Test
    public void testRenewalOfDeploymentCertificateWithNullCertAndKey() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                null,
                true
        ).toCompletableFuture().join();

        assertThat(new String(newCert.cert()), is("new-cert0"));
        assertThat(new String(newCert.key()), is("new-key0"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    @Test
    public void testRenewalOfDeploymentCertificateWithRenewingCa() {
        MockedClusterCa mockedCa = new MockedClusterCa();
        mockedCa.setCaCertGeneration(1);

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8));

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                initialCert,
                true
        ).toCompletableFuture().join();

        assertThat(new String(newCert.cert()), is("new-cert0"));
        assertThat(new String(newCert.key()), is("new-key0"));
        assertThat(newCert.caCertGeneration(), is(1));
    }

    @Test
    public void testRenewalOfDeploymentCertificateDelayedRenewal() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8));

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                initialCert,
                true
        ).toCompletableFuture().join();

        assertThat(new String(newCert.cert()), is("new-cert0"));
        assertThat(new String(newCert.key()), is("new-key0"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    @Test
    public void testRenewalOfDeploymentCertificateDelayedRenewalOutsideOfMaintenanceWindow() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8));

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                initialCert,
                false
        ).toCompletableFuture().join();

        assertThat(new String(newCert.cert()), is(EXPIRED_DUMMY_CERT));
        assertThat(new String(newCert.key()), is("old-key"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    @Test
    public void testHandlingOldSecretWithPKCS12Files() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8),
                null, "old-keystore".getBytes(), "old-password");

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                initialCert,
                true
        ).toCompletableFuture().join();

        assertThat(new String(newCert.cert()), is(DUMMY_CERT));
        assertThat(new String(newCert.key()), is("old-key"));
        assertThat(new String(newCert.keyStore()), is("old-keystore"));
        assertThat(newCert.storePassword(), is("old-password"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    @Test
    public void testIncludesCaChain()  {
        String cert = "CERT";
        String caChain = "CACHAIN";
        String caChain2 = "CA2CHAIN";
        String certWithChain = cert + caChain;

        assertThat(InternalCa.includesCaChain(certWithChain.getBytes(StandardCharsets.US_ASCII), caChain.getBytes(StandardCharsets.US_ASCII)), is(true));
        assertThat(InternalCa.includesCaChain(cert.getBytes(StandardCharsets.US_ASCII), caChain.getBytes(StandardCharsets.US_ASCII)), is(false));
        assertThat(InternalCa.includesCaChain(certWithChain.getBytes(StandardCharsets.US_ASCII), caChain2.getBytes(StandardCharsets.US_ASCII)), is(false));
        assertThat(InternalCa.includesCaChain(certWithChain.getBytes(StandardCharsets.US_ASCII), cert.getBytes(StandardCharsets.US_ASCII)), is(false));
        assertThat(InternalCa.includesCaChain(null, caChain.getBytes(StandardCharsets.US_ASCII)), is(false));
        assertThat(InternalCa.includesCaChain(cert.getBytes(StandardCharsets.US_ASCII), null), is(false));
    }

    public static class MockedClusterCa extends InternalCa {
        private final AtomicInteger invocationCount = new AtomicInteger(0);

        public MockedClusterCa() {
            super(Reconciliation.DUMMY_RECONCILIATION, CaRole.CLUSTER_CA, null, null, null, null, CaConfig.createDefault());
        }

        @Override
        public byte[] currentCaCertBytes() {
            return "CA-CERT".getBytes();
        }

        @Override
        public CertAndKey generateSignedCert(Subject subject,
                                                File csrFile, File keyFile, File certFile, File keyStoreFile, boolean includeCaChain) {
            int index = invocationCount.getAndIncrement();

            byte[] cert;
            if (includeCaChain) {
                // Simulate concatenated chain: leaf + CA
                cert = ("new-cert" + index + "CA-CERT").getBytes();
            } else {
                cert = ("new-cert" + index).getBytes();
            }

            return new CertAndKey(
                    ("new-key" + index).getBytes(),
                    cert,
                    ("new-truststore" + index).getBytes(),
                    ("new-keystore" + index).getBytes(),
                    "new-password" + index,
                    caCertGeneration
            );
        }

        public void setCaCertGeneration(int value) {
            this.caCertGeneration = value;
        }
    }
}