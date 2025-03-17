/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.ca;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.MockCertIssuer;
import io.strimzi.operator.user.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class InternalCaUserCertIssuerTest {
    // Certificate used for expiration tests where actual expiration is needed. This certificate expires on 27th March 2023.
    // But with correct configuration or renewal days before expiration, it can be used to trigger expiration,
    private final static byte[] USER_CRT_FOR_EXPIRATION_TEST = ("-----BEGIN CERTIFICATE-----\n" +
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
            "-----END CERTIFICATE-----\n").getBytes(StandardCharsets.UTF_8);

    private final Secret clientsCaCert = ResourceUtils.createClientsCaCertSecret(ResourceUtils.NAMESPACE);
    private final Secret clientsCaKey = ResourceUtils.createClientsCaKeySecret(ResourceUtils.NAMESPACE);
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");

    @Test
    public void testNewUser() {
        InternalCaUserCertIssuer provider = new InternalCaUserCertIssuer(new MockCertIssuer(), passwordGenerator, null, Clock.systemUTC());
        UserCertResult result = provider.maybeCopyOrGenerateCert(
                Reconciliation.DUMMY_RECONCILIATION, clientsCaCert, clientsCaKey,
                null, ResourceUtils.NAME, 365, 30, true, null)
                .toCompletableFuture().join();

        assertThat(result.caCertBase64(), is(clientsCaCert.getData().get("ca.crt")));
        assertThat(result.userCertAndKey(), notNullValue());
    }

    @Test
    public void testExistingUserWithForceRenewAnnotation() {
        Secret userSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true")
                .endMetadata()
                .withData(Map.of("ca.crt", clientsCaCert.getData().get("ca.crt"),
                        "user.crt", MockCertIssuer.clientsCaCert(),
                        "user.key", MockCertIssuer.clientsCaKey()))
                .build();

        InternalCaUserCertIssuer provider = new InternalCaUserCertIssuer(new MockCertIssuer(), passwordGenerator, null, Clock.systemUTC());
        UserCertResult result = provider.maybeCopyOrGenerateCert(
                Reconciliation.DUMMY_RECONCILIATION, clientsCaCert, clientsCaKey,
                userSecret, ResourceUtils.NAME, 365, 30, true, null)
                .toCompletableFuture().join();

        assertThat(result.caCertBase64(), is(clientsCaCert.getData().get("ca.crt")));
        assertThat(result.userCertAndKey().certAsBase64String(), not(MockCertIssuer.clientsCaCert()));
        assertThat(result.userCertAndKey().keyAsBase64String(), not(MockCertIssuer.clientsCaKey()));
    }

    @Test
    public void testExistingUserWithIncompleteSecret() {
        String oldPassword = Base64.getEncoder().encodeToString("123456".getBytes(StandardCharsets.UTF_8));
        Secret userSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .withData(Map.of("password", oldPassword))
                .build();

        InternalCaUserCertIssuer provider = new InternalCaUserCertIssuer(new MockCertIssuer(), passwordGenerator, null, Clock.systemUTC());
        UserCertResult result = provider.maybeCopyOrGenerateCert(
                Reconciliation.DUMMY_RECONCILIATION, clientsCaCert, clientsCaKey,
                userSecret, ResourceUtils.NAME, 365, 30, true, null)
                .toCompletableFuture().join();

        assertThat(result.caCertBase64(), is(clientsCaCert.getData().get("ca.crt")));
        assertThat(result.userCertAndKey().storePasswordAsBase64String(), not(oldPassword));
    }

    @Test
    public void testExistingUserWithCompleteSecretButOldCa() {
        byte[] oldCaCert = "Some old CA public key".getBytes(StandardCharsets.UTF_8);
        byte[] oldUserCrt = "User public key".getBytes(StandardCharsets.UTF_8);
        byte[] oldUserKey = "User private key".getBytes(StandardCharsets.UTF_8);
        Secret userSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .withData(Map.of("ca.crt", Base64.getEncoder().encodeToString(oldCaCert),
                        "user.crt", Base64.getEncoder().encodeToString(oldUserCrt),
                        "user.key", Base64.getEncoder().encodeToString(oldUserKey)))
                .build();

        InternalCaUserCertIssuer provider = new InternalCaUserCertIssuer(new MockCertIssuer(), passwordGenerator, null, Clock.systemUTC());
        UserCertResult result = provider.maybeCopyOrGenerateCert(
                Reconciliation.DUMMY_RECONCILIATION, clientsCaCert, clientsCaKey,
                userSecret, ResourceUtils.NAME, 365, 30, true, null)
                .toCompletableFuture().join();

        assertThat(result.caCertBase64(), not(oldCaCert));
        assertThat(result.userCertAndKey().cert(), not(oldUserCrt));
        assertThat(result.userCertAndKey().key(), not(oldUserKey));
    }

    @Test
    public void testExistingUserWithCompleteSecret() {
        Secret userSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .withData(Map.of("ca.crt", clientsCaCert.getData().get("ca.crt"),
                        "user.crt", MockCertIssuer.clientsCaCert(),
                        "user.key", MockCertIssuer.clientsCaKey()))
                .build();

        InternalCaUserCertIssuer provider = new InternalCaUserCertIssuer(new MockCertIssuer(), passwordGenerator, null, Clock.systemUTC());
        UserCertResult result = provider.maybeCopyOrGenerateCert(
                Reconciliation.DUMMY_RECONCILIATION, clientsCaCert, clientsCaKey,
                userSecret, ResourceUtils.NAME, 365, 30, true, null)
                .toCompletableFuture().join();

        assertThat(result.caCertBase64(), is(clientsCaCert.getData().get("ca.crt")));
        assertThat(result.userCertAndKey().certAsBase64String(), is(MockCertIssuer.clientsCaCert()));
        assertThat(result.userCertAndKey().keyAsBase64String(), is(MockCertIssuer.clientsCaKey()));
    }

    @Test
    public void testExistingUserWithExpiringCertWithoutMaintenanceWindows() {
        Secret userSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .withData(Map.of("ca.crt", clientsCaCert.getData().get("ca.crt"),
                        "user.crt", Base64.getEncoder().encodeToString(USER_CRT_FOR_EXPIRATION_TEST),
                        "user.key", Base64.getEncoder().encodeToString("User private key".getBytes(StandardCharsets.UTF_8))))
                .build();

        InternalCaUserCertIssuer provider = new InternalCaUserCertIssuer(new MockCertIssuer(), passwordGenerator, null, Clock.systemUTC());
        UserCertResult result = provider.maybeCopyOrGenerateCert(
                Reconciliation.DUMMY_RECONCILIATION, clientsCaCert, clientsCaKey,
                userSecret, ResourceUtils.NAME, 1000, 500, true, null)
                .toCompletableFuture().join();

        assertThat(result.userCertAndKey().cert(), not(USER_CRT_FOR_EXPIRATION_TEST));
    }

    @Test
    public void testExistingUserWithExpiringCertInMaintenanceWindow() {
        Secret userSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .withData(Map.of("ca.crt", clientsCaCert.getData().get("ca.crt"),
                        "user.crt", Base64.getEncoder().encodeToString(USER_CRT_FOR_EXPIRATION_TEST),
                        "user.key", Base64.getEncoder().encodeToString("User private key".getBytes(StandardCharsets.UTF_8))))
                .build();

        InternalCaUserCertIssuer provider = new InternalCaUserCertIssuer(new MockCertIssuer(), passwordGenerator,
                List.of("* * 8-10 * * ?", "* * 14-15 * * ?"),
                Clock.fixed(Instant.parse("2018-11-26T09:00:00Z"), Clock.systemUTC().getZone()));
        UserCertResult result = provider.maybeCopyOrGenerateCert(
                Reconciliation.DUMMY_RECONCILIATION, clientsCaCert, clientsCaKey,
                userSecret, ResourceUtils.NAME, 1000, 500, true, null)
                .toCompletableFuture().join();

        assertThat(result.userCertAndKey().cert(), not(USER_CRT_FOR_EXPIRATION_TEST));
    }

    @Test
    public void testExistingUserWithExpiringCertOutsideOfMaintenanceWindow() {
        Secret userSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .withData(Map.of("ca.crt", clientsCaCert.getData().get("ca.crt"),
                        "user.crt", Base64.getEncoder().encodeToString(USER_CRT_FOR_EXPIRATION_TEST),
                        "user.key", Base64.getEncoder().encodeToString("User private key".getBytes(StandardCharsets.UTF_8))))
                .build();

        InternalCaUserCertIssuer provider = new InternalCaUserCertIssuer(new MockCertIssuer(), passwordGenerator,
                List.of("* * 8-10 * * ?", "* * 14-15 * * ?"),
                Clock.fixed(Instant.parse("2018-11-26T11:55:00Z"), Clock.systemUTC().getZone()));
        UserCertResult result = provider.maybeCopyOrGenerateCert(
                Reconciliation.DUMMY_RECONCILIATION, clientsCaCert, clientsCaKey,
                userSecret, ResourceUtils.NAME, 1000, 500, true, null)
                .toCompletableFuture().join();

        assertThat(result.userCertAndKey().cert(), is(USER_CRT_FOR_EXPIRATION_TEST));
    }
}
