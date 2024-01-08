/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.common.CertificateExpirationPolicy;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
@ExtendWith(VertxExtension.class)
public class ClusterCaRenewalTest {
    private static final Function<NodeRef, Subject> SUBJECT_FN = node -> new Subject.Builder().build();
    private static final Set<NodeRef> NODES = new LinkedHashSet<>();
    // LinkedHashSet is used to maintain ordering and have predictable test results
    static {
        NODES.add(new NodeRef("pod0", 0, null, false, true));
        NODES.add(new NodeRef("pod1", 1, null, false, true));
        NODES.add(new NodeRef("pod2", 2, null, false, true));
    }

    @ParallelTest
    public void renewalOfCertificatesWithNullSecret() throws IOException {
        ClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, 2, 1, true, null);

        boolean isMaintenanceTimeWindowsSatisfied = true;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                null,
                isMaintenanceTimeWindowsSatisfied);

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
    }

    @ParallelTest
    public void renewalOfCertificatesWithCaRenewal() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, 2, 1, true, null);
        mockedCa.setCertRenewed(true);

        Map<String, String> initialCerts = new HashMap<>();
        initialCerts.put("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod0.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod0.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod1.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod1.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod1.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod1.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod2.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod2.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod2.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod2.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
                
        Secret initialSecret = initialSecret(initialCerts);

        boolean isMaintenanceTimeWindowsSatisfied = true;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialSecret,
                isMaintenanceTimeWindowsSatisfied);

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
    }

    @ParallelTest
    public void renewalOfCertificatesWithBrokenCaRenewal() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, 2, 1, true, null);
        mockedCa.setCaGeneration(2);

        Map<String, String> initialCerts = new HashMap<>();
        initialCerts.put("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod0.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod0.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod1.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod1.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod1.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod1.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod2.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod2.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod2.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod2.password", Base64.getEncoder().encodeToString("old-password".getBytes()));

        Secret initialSecret = initialSecret(1, initialCerts);

        boolean isMaintenanceTimeWindowsSatisfied = true;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialSecret,
                isMaintenanceTimeWindowsSatisfied);

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
    }

    @ParallelTest
    public void renewalOfCertificatesDelayedRenewalInWindow() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, 2, 1, true, null);
        mockedCa.setCertExpiring(true);

        Map<String, String> initialCerts = new HashMap<>();
        initialCerts.put("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod0.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod0.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod1.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod1.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod1.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod1.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod2.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod2.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod2.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod2.password", Base64.getEncoder().encodeToString("old-password".getBytes()));

        Secret initialSecret = initialSecret(initialCerts);

        boolean isMaintenanceTimeWindowsSatisfied = true;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialSecret,
                isMaintenanceTimeWindowsSatisfied);

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
    }

    @ParallelTest
    public void renewalOfCertificatesDelayedRenewalOutsideWindow() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, 2, 1, true, null);
        mockedCa.setCertExpiring(true);

        Map<String, String> initialCerts = new HashMap<>();
        initialCerts.put("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod0.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod0.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod1.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod1.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod1.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod1.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod2.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod2.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod2.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod2.password", Base64.getEncoder().encodeToString("old-password".getBytes()));

        Secret initialSecret = initialSecret(initialCerts);
        
        boolean isMaintenanceTimeWindowsSatisfied = false;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialSecret,
                isMaintenanceTimeWindowsSatisfied);

        assertThat(new String(newCerts.get("pod0").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod0").storePassword(), is("old-password"));

        assertThat(new String(newCerts.get("pod1").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod1").storePassword(), is("old-password"));

        assertThat(new String(newCerts.get("pod2").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod2").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod2").storePassword(), is("old-password"));
    }

    @ParallelTest
    public void renewalOfCertificatesWithNewNodesOutsideWindow() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, 2, 1, true, null);
        mockedCa.setCertExpiring(true);

        Map<String, String> initialCerts = new HashMap<>();
        initialCerts.put("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod0.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod0.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod1.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod1.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod1.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod1.password", Base64.getEncoder().encodeToString("old-password".getBytes()));

        Secret initialSecret = initialSecret(initialCerts);

        boolean isMaintenanceTimeWindowsSatisfied = false;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialSecret,
                isMaintenanceTimeWindowsSatisfied);

        assertThat(new String(newCerts.get("pod0").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod0").storePassword(), is("old-password"));

        assertThat(new String(newCerts.get("pod1").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod1").storePassword(), is("old-password"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password0"));
    }

    @ParallelTest
    public void noRenewal() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, 2, 1, true, null);

        Map<String, String> initialCerts = new HashMap<>();
        initialCerts.put("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod0.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod0.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod1.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod1.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod1.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod1.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod2.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod2.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod2.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod2.password", Base64.getEncoder().encodeToString("old-password".getBytes()));

        Secret initialSecret = initialSecret(initialCerts);

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialSecret,
                true);

        assertThat(new String(newCerts.get("pod0").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod0").storePassword(), is("old-password"));

        assertThat(new String(newCerts.get("pod1").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod1").storePassword(), is("old-password"));

        assertThat(new String(newCerts.get("pod2").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod2").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod2").storePassword(), is("old-password"));
    }

    @ParallelTest
    public void noRenewalWithScaleUp() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, 2, 1, true, null);

        Map<String, String> initialCerts = new HashMap<>();
        initialCerts.put("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod0.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod0.password", Base64.getEncoder().encodeToString("old-password".getBytes()));

        Secret initialSecret = initialSecret(initialCerts);

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialSecret,
                true);

        assertThat(new String(newCerts.get("pod0").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod0").storePassword(), is("old-password"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password0"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password1"));
    }

    @ParallelTest
    public void noRenewalWithScaleUpInTheMiddle() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, 2, 1, true, null);

        Map<String, String> initialCerts = new HashMap<>();
        initialCerts.put("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod0.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod0.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod2.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod2.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod2.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod2.password", Base64.getEncoder().encodeToString("old-password".getBytes()));

        Secret initialSecret = initialSecret(initialCerts);

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialSecret,
                true);

        assertThat(new String(newCerts.get("pod0").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod0").storePassword(), is("old-password"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password0"));

        assertThat(new String(newCerts.get("pod2").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod2").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod2").storePassword(), is("old-password"));
    }

    @ParallelTest
    public void noRenewalScaleDown() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, 2, 1, true, null);

        Map<String, String> initialCerts = new HashMap<>();
        initialCerts.put("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod0.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod0.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod1.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod1.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod1.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod1.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod2.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod2.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod2.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod2.password", Base64.getEncoder().encodeToString("old-password".getBytes()));

        Secret initialSecret = initialSecret(initialCerts);

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                Set.of(new NodeRef("pod1", 1, null, false, true)),
                SUBJECT_FN,
                initialSecret,
                true);

        assertThat(newCerts.get("pod0"), is(nullValue()));

        assertThat(new String(newCerts.get("pod1").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("old-keystore"));
        assertThat(newCerts.get("pod1").storePassword(), is("old-password"));

        assertThat(newCerts.get("pod2"), is(nullValue()));
    }

    @ParallelTest
    public void oldVersionWithoutPkcs12() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, 2, 1, true, null);

        Map<String, String> initialCerts = new HashMap<>();
        initialCerts.put("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod1.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod1.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod2.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod2.key", Base64.getEncoder().encodeToString("old-key".getBytes()));

        Secret initialSecret = initialSecret(initialCerts);

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialSecret,
                true);

        assertThat(new String(newCerts.get("pod0").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));

        assertThat(new String(newCerts.get("pod1").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));

        assertThat(new String(newCerts.get("pod2").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod2").key()), is("old-key"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
    }

    @ParallelTest
    public void changedSubject() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, null, 2, 1, true, null);
        mockedCa.setCertExpiring(true);

        Map<String, String> initialCerts = new HashMap<>();
        initialCerts.put("pod0.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod0.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod0.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod0.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod1.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod1.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod1.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod1.password", Base64.getEncoder().encodeToString("old-password".getBytes()));
        initialCerts.put("pod2.crt", Base64.getEncoder().encodeToString("old-cert".getBytes()));
        initialCerts.put("pod2.key", Base64.getEncoder().encodeToString("old-key".getBytes()));
        initialCerts.put("pod2.p12", Base64.getEncoder().encodeToString("old-keystore".getBytes()));
        initialCerts.put("pod2.password", Base64.getEncoder().encodeToString("old-password".getBytes()));

        Secret initialSecret = initialSecret(initialCerts);

        boolean isMaintenanceTimeWindowsSatisfied = true;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                node -> new Subject.Builder().withCommonName(node.podName()).build(),
                initialSecret,
                isMaintenanceTimeWindowsSatisfied);

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
    }

    public static Secret initialSecret(Map<String, String> data)   {
        return initialSecret(0, data);
    }

    public static Secret initialSecret(int generation, Map<String, String> data)   {
        return new SecretBuilder()
                .withNewMetadata()
                    .withNamespace("test-namespace")
                    .withName("test-secret")
                    .withAnnotations(Map.of(ClusterCa.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(generation)))
                .endMetadata()
                .withData(data)
                .build();
    }

    public static class MockedClusterCa extends ClusterCa {
        private final AtomicInteger invocationCount = new AtomicInteger(0);
        private boolean isCertRenewed;
        private boolean isCertExpiring;
        private int caGeneration = 0;

        public MockedClusterCa(Reconciliation reconciliation, CertManager certManager, PasswordGenerator passwordGenerator, String commonName, Secret caCertSecret, Secret caKeySecret, int validityDays, int renewalDays, boolean generateCa, CertificateExpirationPolicy policy) {
            super(reconciliation, certManager, passwordGenerator, commonName, caCertSecret, caKeySecret, validityDays, renewalDays, generateCa, policy);
        }

        @Override
        public boolean certRenewed() {
            return isCertRenewed;
        }

        @Override
        public boolean isExpiring(Secret secret, String certKey)  {
            return isCertExpiring;
        }

        @Override
        protected boolean certSubjectChanged(CertAndKey certAndKey, Subject desiredSubject, String podName)    {
            // When differs from the default we use, we indicate change
            return !new Subject.Builder().build().equals(desiredSubject);
        }

        @Override
        protected CertAndKey generateSignedCert(Subject subject,
                                                File csrFile, File keyFile, File certFile, File keyStoreFile) {
            int index = invocationCount.getAndIncrement();

            return new CertAndKey(
                    ("new-key" + index).getBytes(),
                    ("new-cert" + index).getBytes(),
                    ("new-truststore" + index).getBytes(),
                    ("new-keystore" + index).getBytes(),
                    "new-password" + index
            );
        }

        @Override
        public CertAndKey addKeyAndCertToKeyStore(String alias, byte[] key, byte[] cert) {
            int index = invocationCount.getAndIncrement();

            return new CertAndKey(
                    key,
                    cert,
                    ("new-truststore" + index).getBytes(),
                    ("new-keystore" + index).getBytes(),
                    "new-password" + index);
        }

        @Override
        public int certGeneration() {
            return caGeneration;
        }
        
        public void setCertRenewed(boolean certRenewed) {
            isCertRenewed = certRenewed;
        }

        public void setCertExpiring(boolean certExpiring) {
            isCertExpiring = certExpiring;
        }

        public void setCaGeneration(int caGeneration) {
            this.caGeneration = caGeneration;
        }
    }
}