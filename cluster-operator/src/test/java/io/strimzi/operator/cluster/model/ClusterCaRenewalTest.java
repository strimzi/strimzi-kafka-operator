/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.CertificateExpirationPolicy;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

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

    @Test
    public void renewalOfCertificatesWithNullCertificates() throws IOException {
        ClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);

        boolean isMaintenanceTimeWindowsSatisfied = true;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                null,
                isMaintenanceTimeWindowsSatisfied,
                false
        );

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

    @Test
    public void renewalOfCertificatesWithCaRenewal() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);
        mockedCa.setCaCertGeneration(1);

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));

        boolean isMaintenanceTimeWindowsSatisfied = true;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                isMaintenanceTimeWindowsSatisfied,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));
        assertThat(newCerts.get("pod0").caCertGeneration(), is(1));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));
        assertThat(newCerts.get("pod1").caCertGeneration(), is(1));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
        assertThat(newCerts.get("pod2").caCertGeneration(), is(1));
    }

    @Test
    public void renewalOfCertificatesDelayedRenewalInWindow() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);
        mockedCa.setCertExpiring(true);

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));

        boolean isMaintenanceTimeWindowsSatisfied = true;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                isMaintenanceTimeWindowsSatisfied,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));
        assertThat(new String(newCerts.get("pod0").keyStore()), is("new-keystore0"));
        assertThat(newCerts.get("pod0").storePassword(), is("new-password0"));
        assertThat(newCerts.get("pod0").caCertGeneration(), is(0));


        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));
        assertThat(new String(newCerts.get("pod1").keyStore()), is("new-keystore1"));
        assertThat(newCerts.get("pod1").storePassword(), is("new-password1"));
        assertThat(newCerts.get("pod1").caCertGeneration(), is(0));


        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
        assertThat(new String(newCerts.get("pod2").keyStore()), is("new-keystore2"));
        assertThat(newCerts.get("pod2").storePassword(), is("new-password2"));
        assertThat(newCerts.get("pod2").caCertGeneration(), is(0));

    }

    @Test
    public void renewalOfCertificatesDelayedRenewalOutsideWindow() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);
        mockedCa.setCertExpiring(true);

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        
        boolean isMaintenanceTimeWindowsSatisfied = false;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                isMaintenanceTimeWindowsSatisfied,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));
        assertThat(newCerts.get("pod0").caCertGeneration(), is(0));


        assertThat(new String(newCerts.get("pod1").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));
        assertThat(newCerts.get("pod1").caCertGeneration(), is(0));


        assertThat(new String(newCerts.get("pod2").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod2").key()), is("old-key"));
        assertThat(newCerts.get("pod1").caCertGeneration(), is(0));

    }

    @Test
    public void renewalOfCertificatesWithNewNodesOutsideWindow() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);
        mockedCa.setCertExpiring(true);

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));

        boolean isMaintenanceTimeWindowsSatisfied = false;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                isMaintenanceTimeWindowsSatisfied,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));

        assertThat(new String(newCerts.get("pod1").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key0"));
    }

    @Test
    public void noRenewalOfCertificates() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                true,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));

        assertThat(new String(newCerts.get("pod1").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));

        assertThat(new String(newCerts.get("pod2").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod2").key()), is("old-key"));
    }

    @Test
    public void nosRenewalOfCertificatesWithScaleUp() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                true,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key0"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key1"));
    }

    @Test
    public void noRenewalOfCertificatesWithScaleUpInTheMiddle() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                true,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod0").key()), is("old-key"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key0"));

        assertThat(new String(newCerts.get("pod2").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod2").key()), is("old-key"));
    }

    @Test
    public void noRenewalOfCertificatesScaleDown() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                Set.of(new NodeRef("pod1", 1, null, false, true)),
                SUBJECT_FN,
                initialCerts,
                true,
                false
        );

        assertThat(newCerts.get("pod0"), is(nullValue()));

        assertThat(new String(newCerts.get("pod1").cert()), is("old-cert"));
        assertThat(new String(newCerts.get("pod1").key()), is("old-key"));

        assertThat(newCerts.get("pod2"), is(nullValue()));
    }

    @Test
    public void changedSubjectOfCertificates() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);
        mockedCa.setCertExpiring(true);

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod1", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));
        initialCerts.put("pod2", new CertAndKey("old-key".getBytes(), "old-cert".getBytes()));

        boolean isMaintenanceTimeWindowsSatisfied = true;

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                node -> new Subject.Builder().withCommonName(node.podName()).build(),
                initialCerts,
                isMaintenanceTimeWindowsSatisfied,
                false
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0"));
        assertThat(new String(newCerts.get("pod0").key()), is("new-key0"));

        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1"));
        assertThat(new String(newCerts.get("pod1").key()), is("new-key1"));

        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2"));
        assertThat(new String(newCerts.get("pod2").key()), is("new-key2"));
    }


    @Test
    public void certificatesIncludeCaChain() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                null,
                true,
                true
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0CA-CERT"));
        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1CA-CERT"));
        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2CA-CERT"));
    }

    @Test
    public void caChainAddedToExistingCertificates() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);

        Map<String, CertAndKey> initialCerts = new HashMap<>();
        initialCerts.put("pod0", new CertAndKey("new-key0".getBytes(), "new-cert0".getBytes()));
        initialCerts.put("pod1", new CertAndKey("new-key1".getBytes(), "new-cert1".getBytes()));
        initialCerts.put("pod2", new CertAndKey("new-key2".getBytes(), "new-cert2".getBytes()));

        Map<String, CertAndKey> newCerts = mockedCa.maybeCopyOrGenerateServerCerts(
                Reconciliation.DUMMY_RECONCILIATION,
                NODES,
                SUBJECT_FN,
                initialCerts,
                true,
                true
        );

        assertThat(new String(newCerts.get("pod0").cert()), is("new-cert0CA-CERT"));
        assertThat(new String(newCerts.get("pod1").cert()), is("new-cert1CA-CERT"));
        assertThat(new String(newCerts.get("pod2").cert()), is("new-cert2CA-CERT"));
    }

    @Test
    public void testRenewalOfDeploymentCertificateWithNullCertAndKey() {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                null,
                true
        );

        assertThat(new String(newCert.cert()), is("new-cert0"));
        assertThat(new String(newCert.key()), is("new-key0"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    @Test
    public void testRenewalOfDeploymentCertificateWithRenewingCa() {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);
        mockedCa.setCaCertGeneration(1);

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), "old-cert".getBytes());

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                initialCert,
                true
        );

        assertThat(new String(newCert.cert()), is("new-cert0"));
        assertThat(new String(newCert.key()), is("new-key0"));
        assertThat(newCert.caCertGeneration(), is(1));
    }

    @Test
    public void testRenewalOfDeploymentCertificateDelayedRenewal() {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);
        mockedCa.setCertExpiring(true);

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), "old-cert".getBytes());

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                initialCert,
                true
        );

        assertThat(new String(newCert.cert()), is("new-cert0"));
        assertThat(new String(newCert.key()), is("new-key0"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    @Test
    public void testRenewalOfDeploymentCertificateDelayedRenewalOutsideOfMaintenanceWindow() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);
        mockedCa.setCertExpiring(true);

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), "old-cert".getBytes());

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                initialCert,
                false
        );

        assertThat(new String(newCert.cert()), is("old-cert"));
        assertThat(new String(newCert.key()), is("old-key"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    @Test
    public void testHandlingOldSecretWithPKCS12Files() throws IOException {
        MockedClusterCa mockedCa = new MockedClusterCa(Reconciliation.DUMMY_RECONCILIATION, null, null, null, null, 2, 1, true, null);

        CertAndKey initialCert = new CertAndKey("old-key".getBytes(), "old-cert".getBytes(), null, "old-keystore".getBytes(), "old-password");

        CertAndKey newCert = mockedCa.maybeCopyOrGenerateClientCert(
                Reconciliation.DUMMY_RECONCILIATION,
                "deployment",
                initialCert,
                true
        );

        assertThat(new String(newCert.cert()), is("old-cert"));
        assertThat(new String(newCert.key()), is("old-key"));
        assertThat(new String(newCert.keyStore()), is("old-keystore"));
        assertThat(newCert.storePassword(), is("old-password"));
        assertThat(newCert.caCertGeneration(), is(0));
    }

    public static class MockedClusterCa extends ClusterCa {
        private final AtomicInteger invocationCount = new AtomicInteger(0);
        private int caCertGeneration;
        private boolean isCertExpiring;

        public MockedClusterCa(Reconciliation reconciliation, CertManager certManager, PasswordGenerator passwordGenerator, Secret caCertSecret, Secret caKeySecret, int validityDays, int renewalDays, boolean generateCa, CertificateExpirationPolicy policy) {
            super(reconciliation, certManager, passwordGenerator, caCertSecret, caKeySecret, validityDays, renewalDays, generateCa, policy);
        }

        @Override
        public byte[] currentCaCertBytes() {
            return "CA-CERT".getBytes();
        }

        @Override
        public boolean isExpiring(byte[] certificate)  {
            return isCertExpiring;
        }

        @Override
        protected boolean certSubjectChanged(CertAndKey certAndKey, Subject desiredSubject, String podName)    {
            // When differs from the default we use, we indicate change
            return !new Subject.Builder().build().equals(desiredSubject);
        }

        @Override
        protected CertAndKey generateSignedCert(Subject subject,
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
        public int caCertGeneration() {
            return caCertGeneration;
        }

        public void setCertExpiring(boolean certExpiring) {
            isCertExpiring = certExpiring;
        }

        public void setCaCertGeneration(int value) {
            caCertGeneration = value;
        }
    }
}