/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.CertificateExpirationPolicy;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class ClusterCaTest {

    private final String namespace = "test";
    private final String cluster = "my-cluster";

    @ParallelTest
    public void testRemoveExpiredCertificate() {
        // simulate certificate creation at following time, with expire at 365 days later (by default)
        String instantExpected = "2022-03-23T09:00:00Z";
        Clock clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        ClusterCa clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(clock), new PasswordGenerator(10, "a", "a"), cluster, null, null);
        clusterCa.setClock(clock);
        clusterCa.createRenewOrReplace(namespace, cluster, emptyMap(), emptyMap(), emptyMap(), null, true);
        assertThat(clusterCa.caCertSecret().getData().size(), is(3));

        // force key replacement so certificate renewal ...
        Secret caKeySecretWithReplaceAnno = new SecretBuilder(clusterCa.caKeySecret())
                .editMetadata()
                .addToAnnotations(Ca.ANNO_STRIMZI_IO_FORCE_REPLACE, "true")
                .endMetadata()
                .build();
        // ... simulated at the following time, with expire at 365 days later (by default)
        instantExpected = "2022-03-23T11:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(clock), new PasswordGenerator(10, "a", "a"), cluster, clusterCa.caCertSecret(), caKeySecretWithReplaceAnno);
        clusterCa.setClock(clock);
        clusterCa.createRenewOrReplace(namespace, cluster, emptyMap(), emptyMap(), emptyMap(), null, true);
        assertThat(clusterCa.caCertSecret().getData().size(), is(4));
        assertThat(clusterCa.caCertSecret().getData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(true));

        // running a CA reconcile simulated at following time (365 days later) expecting expired certificate being removed
        instantExpected = "2023-03-23T10:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(), new PasswordGenerator(10, "a", "a"), cluster, clusterCa.caCertSecret(), clusterCa.caKeySecret());
        clusterCa.setClock(clock);
        clusterCa.createRenewOrReplace(namespace, cluster, emptyMap(), emptyMap(), emptyMap(), null, true);
        assertThat(clusterCa.caCertSecret().getData().size(), is(3));
        assertThat(clusterCa.caCertSecret().getData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(false));
    }

    @ParallelTest
    public void testIsExpiringCertificate() {
        // simulate certificate creation at following time, with expire at 365 days later (by default) and renewal days at 30 (by default)
        String instantExpected = "2022-03-30T09:00:00Z";
        Clock clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        ClusterCa clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(clock), new PasswordGenerator(10, "a", "a"), cluster, null, null);
        clusterCa.setClock(clock);
        clusterCa.createRenewOrReplace(namespace, cluster, emptyMap(), emptyMap(), emptyMap(), null, true);

        // check certificate expiration out of the renewal period, certificate is not expiring
        instantExpected = "2023-02-15T09:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());
        clusterCa.setClock(clock);
        assertThat(clusterCa.isExpiring(clusterCa.caCertSecret(), Ca.CA_CRT), is(false));

        // check certificate expiration within the renewal period, certificate is expiring
        instantExpected = "2023-03-15T09:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());
        clusterCa.setClock(clock);
        assertThat(clusterCa.isExpiring(clusterCa.caCertSecret(), Ca.CA_CRT), is(true));
    }

    @ParallelTest
    public void testRemoveOldCertificate() {
        // simulate certificate creation at following time, with expire at 365 days later (by default)
        String instantExpected = "2022-03-23T09:00:00Z";
        Clock clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        ClusterCa clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(clock), new PasswordGenerator(10, "a", "a"), cluster, null, null);
        clusterCa.setClock(clock);
        clusterCa.createRenewOrReplace(namespace, cluster, emptyMap(), emptyMap(), emptyMap(), null, true);
        assertThat(clusterCa.caCertSecret().getData().size(), is(3));

        // force key replacement so certificate renewal ...
        Secret caKeySecretWithReplaceAnno = new SecretBuilder(clusterCa.caKeySecret())
                .editMetadata()
                .addToAnnotations(Ca.ANNO_STRIMZI_IO_FORCE_REPLACE, "true")
                .endMetadata()
                .build();
        // ... simulated at the following time, with expire at 365 days later (by default)
        instantExpected = "2022-03-23T11:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        clusterCa = new ClusterCa(Reconciliation.DUMMY_RECONCILIATION, new OpenSslCertManager(clock), new PasswordGenerator(10, "a", "a"), cluster, clusterCa.caCertSecret(), caKeySecretWithReplaceAnno);
        clusterCa.setClock(clock);
        clusterCa.createRenewOrReplace(namespace, cluster, emptyMap(), emptyMap(), emptyMap(), null, true);
        assertThat(clusterCa.caCertSecret().getData().size(), is(4));
        assertThat(clusterCa.caCertSecret().getData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(true));

        clusterCa.maybeDeleteOldCerts();
        assertThat(clusterCa.caCertSecret().getData().size(), is(3));
        assertThat(clusterCa.caCertSecret().getData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(false));
    }

    @ParallelTest
    public void testNotRemoveOldCertificateWithCustomCa() {
        Map<String, String> clusterCaCertData = new HashMap<>();
        clusterCaCertData.put(Ca.CA_CRT, Base64.getEncoder().encodeToString("dummy-crt".getBytes()));
        clusterCaCertData.put(Ca.CA_STORE, Base64.getEncoder().encodeToString("dummy-p12".getBytes()));
        clusterCaCertData.put(Ca.CA_STORE_PASSWORD, Base64.getEncoder().encodeToString("dummy-password".getBytes()));

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

        // simulate a renewal with new private key ...
        clusterCaKeyData.put(Ca.CA_KEY, Base64.getEncoder().encodeToString("new-dummy-key".getBytes()));
        clusterCaKey.setData(clusterCaKeyData);
        // ... also saving the old certificate
        clusterCaCertData.put("ca-2023-03-23T09-00-00Z.crt", clusterCaCertData.get(Ca.CA_CRT));
        clusterCaCertData.put(Ca.CA_CRT, Base64.getEncoder().encodeToString("new-dummy-crt".getBytes()));
        clusterCaCertData.put(Ca.CA_STORE, Base64.getEncoder().encodeToString("updated-dummy-p12".getBytes()));
        clusterCaCert.setData(clusterCaCertData);

        clusterCa.maybeDeleteOldCerts();

        // checking that the cluster CA related Secret was not touched by the operator
        Map<String, String> clusterCaCertDataInSecret = clusterCa.caCertSecret().getData();
        assertThat(clusterCaCertDataInSecret.size(), is(4));
        assertThat(new String(Base64.getDecoder().decode(clusterCaCertDataInSecret.get(Ca.CA_CRT))).equals("new-dummy-crt"), is(true));
        assertThat(new String(Base64.getDecoder().decode(clusterCaCertDataInSecret.get(Ca.CA_STORE))).equals("updated-dummy-p12"), is(true));
        assertThat(new String(Base64.getDecoder().decode(clusterCaCertDataInSecret.get(Ca.CA_STORE_PASSWORD))).equals("dummy-password"), is(true));
        assertThat(new String(Base64.getDecoder().decode(clusterCaCertDataInSecret.get("ca-2023-03-23T09-00-00Z.crt"))).equals("dummy-crt"), is(true));
    }
}
