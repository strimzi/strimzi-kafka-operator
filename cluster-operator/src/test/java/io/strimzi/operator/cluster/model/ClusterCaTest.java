/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.common.CertificateExpirationPolicy;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.PasswordGenerator;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ClusterCaTest {
    private final String cluster = "my-cluster";

    @Test
    public void testRemoveExpiredCertificate() {
        // simulate certificate creation at following time, with expire at 365 days later (by default)
        String instantExpected = "2022-03-23T09:00:00Z";
        Clock clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        ClusterCa clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(clock), new PasswordGenerator(10, "a", "a"), cluster, null, null);
        clusterCa.setClock(clock);
        clusterCa.createRenewOrReplace(true, false, false);
        assertThat(clusterCa.caCertData().size(), is(3));

        // ... simulated at the following time, with expire at 365 days later (by default)
        instantExpected = "2022-03-23T11:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(clock), new PasswordGenerator(10, "a", "a"), cluster, buildCertSecret(clusterCa), buildKeySecret(clusterCa));
        clusterCa.setClock(clock);
        // force key replacement so certificate renewal ...
        clusterCa.createRenewOrReplace(true, true, false);
        assertThat(clusterCa.caCertData().size(), is(4));
        assertThat(clusterCa.caCertData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(true));

        // running a CA reconcile simulated at following time (365 days later) expecting expired certificate being removed
        instantExpected = "2023-03-23T10:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(), new PasswordGenerator(10, "a", "a"), cluster, buildCertSecret(clusterCa), buildKeySecret(clusterCa));
        clusterCa.setClock(clock);
        clusterCa.createRenewOrReplace(true, false, false);
        assertThat(clusterCa.caCertData().size(), is(3));
        assertThat(clusterCa.caCertData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(false));
    }

    @Test
    public void testIsExpiringCertificate() {
        // simulate certificate creation at following time, with expire at 365 days later (by default) and renewal days at 30 (by default)
        String instantExpected = "2022-03-30T09:00:00Z";
        Clock clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        ClusterCa clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(clock), new PasswordGenerator(10, "a", "a"), cluster, null, null);
        clusterCa.setClock(clock);
        clusterCa.createRenewOrReplace(true, false, false);

        // check certificate expiration out of the renewal period, certificate is not expiring
        instantExpected = "2023-02-15T09:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());
        clusterCa.setClock(clock);
        assertThat(clusterCa.isExpiring(buildCertSecret(clusterCa), Ca.CA_CRT), is(false));

        // check certificate expiration within the renewal period, certificate is expiring
        instantExpected = "2023-03-15T09:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());
        clusterCa.setClock(clock);
        assertThat(clusterCa.isExpiring(buildCertSecret(clusterCa), Ca.CA_CRT), is(true));
    }

    @Test
    public void testRemoveOldCertificate() {
        // simulate certificate creation at following time, with expire at 365 days later (by default)
        String instantExpected = "2022-03-23T09:00:00Z";
        Clock clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        ClusterCa clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(clock), new PasswordGenerator(10, "a", "a"), cluster, null, null);
        clusterCa.setClock(clock);
        clusterCa.createRenewOrReplace(true, false, false);
        assertThat(clusterCa.caCertData().size(), is(3));

        // ... simulated at the following time, with expire at 365 days later (by default)
        instantExpected = "2022-03-23T11:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(clock), new PasswordGenerator(10, "a", "a"), cluster, buildCertSecret(clusterCa), buildKeySecret(clusterCa));
        clusterCa.setClock(clock);
        // force key replacement so certificate renewal ...
        clusterCa.createRenewOrReplace(true, true, false);
        assertThat(clusterCa.caCertData().size(), is(4));
        assertThat(clusterCa.caCertData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(true));

        clusterCa.maybeDeleteOldCerts();
        assertThat(clusterCa.caCertData().size(), is(3));
        assertThat(clusterCa.caCertData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(false));
    }

    @Test
    public void testNotRemoveOldCertificateWithCustomCa() {
        Map<String, String> clusterCaCertData = new HashMap<>();
        clusterCaCertData.put(Ca.CA_CRT, Base64.getEncoder().encodeToString("new-dummy-crt".getBytes()));
        clusterCaCertData.put(Ca.CA_STORE, Base64.getEncoder().encodeToString("dummy-p12".getBytes()));
        clusterCaCertData.put(Ca.CA_STORE_PASSWORD, Base64.getEncoder().encodeToString("dummy-password".getBytes()));
        // simulate old cert still present
        clusterCaCertData.put("ca-2023-03-23T09-00-00Z.crt", Base64.getEncoder().encodeToString("dummy-crt".getBytes()));

        Secret clusterCaCert = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-cluster-cluster-ca-cert")
                .endMetadata()
                .withData(clusterCaCertData)
                .build();

        Map<String, String> clusterCaKeyData = new HashMap<>();
        clusterCaKeyData.put(Ca.CA_KEY, Base64.getEncoder().encodeToString("dummy-key".getBytes()));

        Secret clusterCaKey = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-cluster-cluster-ca")
                .endMetadata()
                .withData(clusterCaKeyData)
                .build();

        ClusterCa clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(), new PasswordGenerator(10, "a", "a"), cluster, clusterCaCert, clusterCaKey, 0, 0, false, CertificateExpirationPolicy.RENEW_CERTIFICATE);

        clusterCa.maybeDeleteOldCerts();

        // checking that the cluster CA related Secret was not touched by the operator
        Map<String, String> clusterCaCertDataInSecret = clusterCa.caCertData();
        assertThat(clusterCaCertDataInSecret.size(), is(4));
        assertThat(Util.decodeFromBase64(clusterCaCertDataInSecret.get(Ca.CA_CRT)).equals("new-dummy-crt"), is(true));
        assertThat(Util.decodeFromBase64(clusterCaCertDataInSecret.get(Ca.CA_STORE)).equals("dummy-p12"), is(true));
        assertThat(Util.decodeFromBase64(clusterCaCertDataInSecret.get(Ca.CA_STORE_PASSWORD)).equals("dummy-password"), is(true));
        assertThat(Util.decodeFromBase64(clusterCaCertDataInSecret.get("ca-2023-03-23T09-00-00Z.crt")).equals("dummy-crt"), is(true));
    }

    private Secret buildCertSecret(Ca ca) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(AbstractModel.clusterCaCertSecretName(cluster))
                    .withAnnotations(Map.of(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, String.valueOf(ca.caCertGeneration())))
                .endMetadata()
                .withData(ca.caCertData())
                .build();
    }

    private Secret buildKeySecret(Ca ca) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(AbstractModel.clusterCaKeySecretName(cluster))
                    .withAnnotations(Map.of(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, String.valueOf(ca.caKeyGeneration())))
                .endMetadata()
                .withData(ca.caKeyData())
                .build();
    }
}
