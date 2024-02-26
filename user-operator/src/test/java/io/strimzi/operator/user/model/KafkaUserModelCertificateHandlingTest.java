/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.user.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaUserModelCertificateHandlingTest {
    // Certificate used for expiration tests where actual expiration is needed. This certificate expires on 27th March 2023.
    // But with correct configuration or renewal days before expiration, it can be used to trigger expiration,
    private final static String USER_CRT_FOR_EXPIRATION_TEST = "-----BEGIN CERTIFICATE-----\n" +
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

    private final Secret clientsCaCert = ResourceUtils.createClientsCaCertSecret(ResourceUtils.NAMESPACE);
    private final Secret clientsCaKey = ResourceUtils.createClientsCaKeySecret(ResourceUtils.NAMESPACE);
    private final CertManager mockCertManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");

    @Test
    public void testNewUser()    {
        MockKafkaUserModel model = new MockKafkaUserModel();
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, null, 365, 30, null, Clock.systemUTC());

        assertThat(model.generateNewCertificateCalled, is(1));
        assertThat(model.reuseCertificateCalled, is(0));
    }

    @Test
    public void testExistingUserWithIncompleteSecret()    {
        Secret userSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .withData(Map.of("password", Base64.getEncoder().encodeToString("123456".getBytes(StandardCharsets.UTF_8))))
                .build();

        MockKafkaUserModel model = new MockKafkaUserModel();
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, userSecret, 365, 30, null, Clock.systemUTC());

        assertThat(model.generateNewCertificateCalled, is(1));
        assertThat(model.reuseCertificateCalled, is(0));
    }

    @Test
    public void testExistingUserWithCompleteSecretButOldCa()    {
        Secret userSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .withData(Map.of("ca.crt", Base64.getEncoder().encodeToString("Some old CA public key".getBytes(StandardCharsets.UTF_8)),
                        "user.crt", Base64.getEncoder().encodeToString("User public key".getBytes(StandardCharsets.UTF_8)),
                        "user.key", Base64.getEncoder().encodeToString("User private key".getBytes(StandardCharsets.UTF_8))))
                .build();

        MockKafkaUserModel model = new MockKafkaUserModel();
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, userSecret, 365, 30, null, Clock.systemUTC());

        assertThat(model.generateNewCertificateCalled, is(1));
        assertThat(model.reuseCertificateCalled, is(0));
    }

    @Test
    public void testExistingUserWithCompleteSecret()    {
        Secret userSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .withData(Map.of("ca.crt", clientsCaCert.getData().get("ca.crt"),
                        "user.crt", Base64.getEncoder().encodeToString("User public key".getBytes(StandardCharsets.UTF_8)),
                        "user.key", Base64.getEncoder().encodeToString("User private key".getBytes(StandardCharsets.UTF_8))))
                .build();

        MockKafkaUserModel model = new MockKafkaUserModel();
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, userSecret, 365, 30, null, Clock.systemUTC());

        assertThat(model.generateNewCertificateCalled, is(0));
        assertThat(model.reuseCertificateCalled, is(1));
    }

    @Test
    public void testExistingUserWithExpiringCertWithoutMaintenanceWindows()    {
        Secret userSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .withData(Map.of("ca.crt", clientsCaCert.getData().get("ca.crt"),
                        "user.crt", Base64.getEncoder().encodeToString(USER_CRT_FOR_EXPIRATION_TEST.getBytes(StandardCharsets.UTF_8)),
                        "user.key", Base64.getEncoder().encodeToString("User private key".getBytes(StandardCharsets.UTF_8))))
                .build();

        MockKafkaUserModel model = new MockKafkaUserModel();
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, userSecret, 1000, 500, null, Clock.systemUTC());

        assertThat(model.generateNewCertificateCalled, is(1));
        assertThat(model.reuseCertificateCalled, is(0));
    }

    @Test
    public void testExistingUserWithExpiringCertInMaintenanceWindow()    {
        Secret userSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .withData(Map.of("ca.crt", clientsCaCert.getData().get("ca.crt"),
                        "user.crt", Base64.getEncoder().encodeToString(USER_CRT_FOR_EXPIRATION_TEST.getBytes(StandardCharsets.UTF_8)),
                        "user.key", Base64.getEncoder().encodeToString("User private key".getBytes(StandardCharsets.UTF_8))))
                .build();

        MockKafkaUserModel model = new MockKafkaUserModel();
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, userSecret, 1000, 500, List.of("* * 8-10 * * ?", "* * 14-15 * * ?"), Clock.fixed(Instant.parse("2018-11-26T09:00:00Z"), Clock.systemUTC().getZone()));

        assertThat(model.generateNewCertificateCalled, is(1));
        assertThat(model.reuseCertificateCalled, is(0));
    }

    @Test
    public void testExistingUserWithExpiringCertOutsideOfMaintenanceWindow()    {
        Secret userSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .withData(Map.of("ca.crt", clientsCaCert.getData().get("ca.crt"),
                        "user.crt", Base64.getEncoder().encodeToString(USER_CRT_FOR_EXPIRATION_TEST.getBytes(StandardCharsets.UTF_8)),
                        "user.key", Base64.getEncoder().encodeToString("User private key".getBytes(StandardCharsets.UTF_8))))
                .build();

        MockKafkaUserModel model = new MockKafkaUserModel();
        model.maybeGenerateCertificates(Reconciliation.DUMMY_RECONCILIATION, mockCertManager, passwordGenerator, clientsCaCert, clientsCaKey, userSecret, 1000, 500, List.of("* * 8-10 * * ?", "* * 14-15 * * ?"), Clock.fixed(Instant.parse("2018-11-26T11:55:00Z"), Clock.systemUTC().getZone()));

        assertThat(model.generateNewCertificateCalled, is(0));
        assertThat(model.reuseCertificateCalled, is(1));
    }

    static class MockKafkaUserModel extends KafkaUserModel {
        public int reuseCertificateCalled = 0;
        public int generateNewCertificateCalled = 0;

        protected MockKafkaUserModel() {
            super(ResourceUtils.NAMESPACE, ResourceUtils.NAME, Labels.EMPTY, null);
        }

        @Override
        CertAndKey generateNewCertificate(Reconciliation reconciliation, Ca clientsCa) {
            generateNewCertificateCalled++;
            return null;
        }

        @Override
        CertAndKey reuseCertificate(Reconciliation reconciliation, Ca clientsCa, Secret userSecret) {
            reuseCertificateCalled++;
            return null;
        }
    }
}
