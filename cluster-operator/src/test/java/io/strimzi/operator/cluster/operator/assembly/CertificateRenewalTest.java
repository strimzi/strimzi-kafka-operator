/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.TlsCertificates;
import io.strimzi.api.kafka.model.TlsCertificatesBuilder;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.Ca.CA_CRT;
import static io.strimzi.operator.cluster.model.Ca.CA_KEY;
import static io.strimzi.test.TestUtils.set;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class CertificateRenewalTest {

    public static final String NAMESPACE = "test";
    public static final String NAME = "my-kafka";
    private Vertx vertx = Vertx.vertx();
    private OpenSslCertManager certManager = new OpenSslCertManager();
    private List<Secret> secrets = new ArrayList();

    @Before
    public void clearSecrets() {
        secrets = new ArrayList();
    }

    private ArgumentCaptor<Secret> reconcileCa(TestContext context, TlsCertificates tlsCertificates) {
        SecretOperator secretOps = mock(SecretOperator.class);

        when(secretOps.list(eq(NAMESPACE), any())).thenAnswer(invocation -> {
            Map<String, String> requiredLabels = ((Labels) invocation.getArgument(1)).toMap();
            return secrets.stream().filter(s -> {
                Map<String, String> labels = new HashMap(s.getMetadata().getLabels());
                labels.keySet().retainAll(requiredLabels.keySet());
                return labels.equals(requiredLabels);
            }).collect(Collectors.toList());
        });
        ArgumentCaptor<Secret> c = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(eq(NAMESPACE), eq(AbstractModel.getClusterCaName(NAME)), c.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop()));
        when(secretOps.reconcile(eq(NAMESPACE), eq(AbstractModel.getClusterCaKeyName(NAME)), c.capture())).thenReturn(Future.succeededFuture(ReconcileResult.noop()));

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, false, 1L, certManager,
                new ResourceOperatorSupplier(null, null, null,
                        null, null, secretOps, null, null,
                        null, null, null, null, null));
        Reconciliation reconciliation = new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, NAME);

        Kafka kafka = new KafkaBuilder()
                .editOrNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withTlsCertificates(tlsCertificates)
                .endSpec()
            .build();

        AtomicReference<Throwable> error = new AtomicReference<>();
        Async async = context.async();
        op.new ReconciliationState(reconciliation, kafka).reconcileClusterCa().setHandler(ar -> {
            error.set(ar.cause());
            async.complete();
        });
        async.await();
        if (error.get() != null) {
            Throwable t = error.get();
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof Error) {
                throw (Error) t;
            } else {
                throw new RuntimeException(t);
            }
        }
        return c;
    }

    private CertAndKey generateCa(OpenSslCertManager certManager, TlsCertificates tlsCertificates, String commonName) throws IOException {
        File clusterCaKeyFile = File.createTempFile("tls", "cluster-ca-key");
        File clusterCaCertFile = File.createTempFile("tls", "cluster-ca-cert");
        try {
            Subject sbj = new Subject();
            sbj.setOrganizationName("io.strimzi");
            sbj.setCommonName(commonName);

            certManager.generateSelfSignedCert(clusterCaKeyFile, clusterCaCertFile, sbj, ModelUtils.getCertificateValidity(tlsCertificates));
            return new CertAndKey(Files.readAllBytes(clusterCaKeyFile.toPath()),
                    Files.readAllBytes(clusterCaCertFile.toPath()));
        } finally {
            clusterCaKeyFile.delete();
            clusterCaCertFile.delete();
        }
    }

    private Secret initialCaCertSecret(TlsCertificates tlsCertificates) throws IOException {
        String commonName = "cluster-ca";
        CertAndKey result = generateCa(certManager, tlsCertificates, commonName);
        return ResourceUtils.createInitialClusterCaCertSecret(NAMESPACE, NAME,
                result.certAsBase64String());
    }

    private Secret initialCaKeySecret(TlsCertificates tlsCertificates) throws IOException {
        String commonName = "cluster-ca";
        CertAndKey result = generateCa(certManager, tlsCertificates, commonName);
        return ResourceUtils.createInitialClusterCaKeySecret(NAMESPACE, NAME,
                result.keyAsBase64String());
    }

    @Test
    public void certsGetGeneratedInitiallyAuto(TestContext context) throws IOException {
        TlsCertificates tlsCertificates = new TlsCertificatesBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();
        secrets.clear();
        ArgumentCaptor<Secret> c = reconcileCa(context, tlsCertificates);
        assertEquals(2, c.getAllValues().size());

        assertEquals(singleton(CA_CRT), c.getAllValues().get(0).getData().keySet());
        assertEquals(singleton(CA_KEY), c.getAllValues().get(1).getData().keySet());
    }

    @Test
    public void certsNotGeneratedInitiallyManual(TestContext context) throws IOException {
        TlsCertificates tlsCertificates = new TlsCertificatesBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(false)
                .build();
        secrets.clear();
        ArgumentCaptor<Secret> c = reconcileCa(context, tlsCertificates);
        assertEquals(c.getAllValues().toString(), 2, c.getAllValues().size());
        assertTrue(c.getAllValues().get(0).getData().isEmpty());
        assertTrue(c.getAllValues().get(1).getData().isEmpty());
    }

    @Test
    public void noCertsGetGeneratedOutsideRenewalPeriodAuto(TestContext context) throws IOException {
        noCertsGetGeneratedOutsideRenewalPeriod(context, true);
    }

    private void noCertsGetGeneratedOutsideRenewalPeriod(TestContext context, boolean generateCertificateAuthority) throws IOException {
        TlsCertificates tlsCertificates = new TlsCertificatesBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(generateCertificateAuthority)
                .build();
        Secret initialCaCertSecret = initialCaCertSecret(tlsCertificates);
        Secret initialCaKeySecret = initialCaKeySecret(tlsCertificates);
        Map<String, String> initialCertData = initialCaCertSecret.getData();
        assertEquals(singleton(CA_CRT), initialCertData.keySet());
        String initialCert = initialCertData.get(CA_CRT);
        assertNotNull(initialCert);
        Map<String, String> initialKeyData = initialCaKeySecret .getData();
        assertEquals(singleton(CA_KEY), initialKeyData.keySet());
        String initialKey = initialKeyData.get(CA_KEY);
        assertNotNull(initialKey);

        secrets.add(initialCaCertSecret);
        secrets.add(initialCaKeySecret);
        ArgumentCaptor<Secret> c = reconcileCa(context, tlsCertificates);

        assertEquals(set(CA_CRT), c.getAllValues().get(0).getData().keySet());
        assertEquals(initialCert, c.getAllValues().get(0).getData().get(CA_CRT));

        assertEquals(set(CA_KEY), c.getAllValues().get(1).getData().keySet());
        assertEquals(initialKey, c.getAllValues().get(1).getData().get(CA_KEY));
    }

    @Test
    public void noCertsGetGeneratedOutsideRenewalPeriodManual(TestContext context) throws IOException {
        noCertsGetGeneratedOutsideRenewalPeriod(context, false);
    }

    @Test
    public void newCertsGetGeneratedWhenInRenewalPeriodAuto(TestContext context) throws IOException {
        TlsCertificates tlsCertificates = new TlsCertificatesBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .build();
        Secret initialCaCertSecret = initialCaCertSecret(tlsCertificates);
        Secret initialCaKeySecret = initialCaKeySecret(tlsCertificates);
        Map<String, String> initialCertData = initialCaCertSecret.getData();
        assertEquals(singleton(CA_CRT), initialCertData.keySet());
        String initialCert = initialCertData.get(CA_CRT);
        assertNotNull(initialCert);
        Map<String, String> initialKeyData = initialCaKeySecret .getData();
        assertEquals(singleton(CA_KEY), initialKeyData.keySet());
        String initialKey = initialKeyData.get(CA_KEY);
        assertNotNull(initialKey);

        secrets.add(initialCaCertSecret);
        secrets.add(initialCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, tlsCertificates);
        assertEquals(2, c.getAllValues().size());
        Map<String, String> certData = c.getAllValues().get(0).getData();
        assertEquals(2, certData.size());
        String newCrt = certData.remove(CA_CRT);
        assertNotNull(newCrt);
        String oldKey = certData.keySet().iterator().next();
        String oldCrt = certData.get(oldKey);
        assertNotNull(oldCrt);
        assertNotEquals(newCrt, oldCrt);
        assertEquals(initialCert, oldCrt);

        Map<String, String> keyData = c.getAllValues().get(1).getData();
        assertEquals(singleton(CA_KEY), keyData.keySet());
        String newKey = keyData.remove(CA_KEY);
        assertNotNull(newKey);
        assertNotEquals(initialKey, newKey);
    }

    @Test
    public void newCertsNotGeneratedWhenInRenewalPeriodManual(TestContext context) throws IOException {
        TlsCertificates tlsCertificates = new TlsCertificatesBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(false)
                .build();
        Secret initialCaCertSecret = initialCaCertSecret(tlsCertificates);
        Secret initialCaKeySecret = initialCaKeySecret(tlsCertificates);
        Map<String, String> initialCertData = initialCaCertSecret.getData();
        assertEquals(singleton(CA_CRT), initialCertData.keySet());
        String initialCert = initialCertData.get(CA_CRT);
        assertNotNull(initialCert);
        Map<String, String> initialKeyData = initialCaKeySecret .getData();
        assertEquals(singleton(CA_KEY), initialKeyData.keySet());
        String initialKey = initialKeyData.get(CA_KEY);
        assertNotNull(initialKey);

        secrets.add(initialCaCertSecret);
        secrets.add(initialCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, tlsCertificates);
        assertEquals(2, c.getAllValues().size());
        Map<String, String> certData = c.getAllValues().get(0).getData();
        assertEquals(initialCertData, certData);
        Map<String, String> keyData = c.getAllValues().get(1).getData();
        assertEquals(initialKeyData, keyData);
    }

    @Test
    public void expiredCertsGetRemovedAuto(TestContext context) throws IOException {
        TlsCertificates tlsCertificates = new TlsCertificatesBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();
        Secret initialCaCertSecret = initialCaCertSecret(tlsCertificates);
        Secret initialCaKeySecret = initialCaKeySecret(tlsCertificates);
        Map<String, String> initialCertData = initialCaCertSecret.getData();
        assertEquals(singleton(CA_CRT), initialCertData.keySet());
        String initialCert = initialCertData.get(CA_CRT);
        initialCertData.put("ca-2018-07-01T09-00-00.crt", "whatever");
        assertNotNull(initialCert);
        Map<String, String> initialKeyData = initialCaKeySecret .getData();
        assertEquals(singleton(CA_KEY), initialKeyData.keySet());
        String initialKey = initialKeyData.get(CA_KEY);
        assertNotNull(initialKey);

        secrets.add(initialCaCertSecret);
        secrets.add(initialCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, tlsCertificates);
        assertEquals(2, c.getAllValues().size());
        Map<String, String> certData = c.getAllValues().get(0).getData();
        assertEquals(certData.keySet().toString(), 1, certData.size());
        assertEquals(initialCert, certData.get(CA_CRT));
        Map<String, String> keyData = c.getAllValues().get(1).getData();
        assertEquals(initialKey, keyData.get(CA_KEY));
    }

    @Test
    public void expiredCertsNotRemovedManual(TestContext context) throws IOException {
        TlsCertificates tlsCertificates = new TlsCertificatesBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(false)
                .build();
        Secret initialCaCertSecret = initialCaCertSecret(tlsCertificates);
        Secret initialCaKeySecret = initialCaKeySecret(tlsCertificates);
        Map<String, String> initialCertData = initialCaCertSecret.getData();
        assertEquals(singleton(CA_CRT), initialCertData.keySet());
        String initialCert = initialCertData.get(CA_CRT);
        initialCertData.put("cluster-ca-2018-07-01T09-00-00.crt", "whatever");
        assertNotNull(initialCert);
        Map<String, String> initialKeyData = initialCaKeySecret .getData();
        assertEquals(singleton(CA_KEY), initialKeyData.keySet());
        String initialKey = initialKeyData.get(CA_KEY);
        assertNotNull(initialKey);

        secrets.add(initialCaCertSecret);
        secrets.add(initialCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, tlsCertificates);
        assertEquals(2, c.getAllValues().size());
        Map<String, String> certData = c.getAllValues().get(0).getData();
        assertEquals(initialCertData, certData);
    }
}
