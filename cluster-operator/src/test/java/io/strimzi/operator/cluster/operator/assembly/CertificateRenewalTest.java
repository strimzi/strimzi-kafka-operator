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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.test.TestUtils.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
        when(secretOps.reconcile(eq(NAMESPACE), eq(AbstractModel.getClusterCaName(NAME)), c.capture())).thenReturn(Future.succeededFuture(null));

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

        Async async = context.async();
        op.new ReconciliationState(reconciliation, kafka).reconcileClusterCa().setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.assertTrue(ar.succeeded());
            async.complete();
        });
        async.await();
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

    private Secret initialSecret(TlsCertificates tlsCertificates) throws IOException {
        String commonName = "cluster-ca";
        CertAndKey result = generateCa(certManager, tlsCertificates, commonName);
        return ResourceUtils.createInitialClusterCaSecret(NAMESPACE, NAME,
                result.keyAsBase64String(),
                result.certAsBase64String());
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
        assertEquals(set("cluster-ca.key", "cluster-ca.crt"), c.getValue().getData().keySet());
        assertNotNull(c.getValue().getData().get("cluster-ca.key"));
        assertNotNull(c.getValue().getData().get("cluster-ca.crt"));
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
        assertTrue(c.getValue().getData().isEmpty());
        // XXX It would be nice to be able to assert on the WARN log message
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
        Secret initialSecret = initialSecret(tlsCertificates);
        secrets.add(initialSecret);
        ArgumentCaptor<Secret> c = reconcileCa(context, tlsCertificates);
        assertEquals(set("cluster-ca.key", "cluster-ca.crt"), c.getValue().getData().keySet());
        assertEquals(initialSecret.getData().get("cluster-ca.key"), c.getValue().getData().get("cluster-ca.key"));
        assertEquals(initialSecret.getData().get("cluster-ca.crt"), c.getValue().getData().get("cluster-ca.crt"));
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
        Secret initialSecret = initialSecret(tlsCertificates);
        secrets.add(initialSecret);
        ArgumentCaptor<Secret> c = reconcileCa(context, tlsCertificates);
        Map<String, String> data = c.getValue().getData();
        assertEquals(4, data.size());
        String expectedNewCrt = data.remove("cluster-ca.crt");
        assertNotNull(expectedNewCrt);
        String expectedNewKey = data.remove("cluster-ca.key");
        assertNotNull(expectedNewKey);
        Iterator<String> iterator = data.keySet().iterator();
        String key = iterator.next();
        if (key.endsWith(".crt")) {
            assertEquals("Expected the original crt to be in the data key with expiry date",
                    initialSecret.getData().get("cluster-ca.crt"), data.get(key));
            assertNotEquals(expectedNewCrt, data.get(key));
            assertEquals(key.replaceAll(".crt$", ".key"), iterator.next());
        } else if (key.endsWith(".key")) {
            assertEquals("Expected the original key to be in the data key with expiry date",
                    initialSecret.getData().get("cluster-ca.key"), data.get(key));
            assertNotEquals(expectedNewKey, data.get(key));
            assertEquals(key.replaceAll(".key$", ".crt"), iterator.next());
        } else {
            fail();
        }
    }

    @Test
    public void newCertsNotGeneratedWhenInRenewalPeriodManual(TestContext context) throws IOException {
        TlsCertificates tlsCertificates = new TlsCertificatesBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(false)
                .build();
        Secret initialSecret = initialSecret(tlsCertificates);
        secrets.add(initialSecret);
        ArgumentCaptor<Secret> c = reconcileCa(context, tlsCertificates);
        Map<String, String> data = c.getValue().getData();
        assertEquals(initialSecret.getData(), data);
    }

    @Test
    public void expiredCertsGetRemovedAuto(TestContext context) throws IOException {
        TlsCertificates tlsCertificates = new TlsCertificatesBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();
        Secret initialSecret = initialSecret(tlsCertificates);
        initialSecret.getData().put("cluster-ca-2018-07-01T09:00:00.crt", "whatever");
        initialSecret.getData().put("cluster-ca-2018-07-01T09:00:00.key", "whatever");
        secrets.add(initialSecret);
        ArgumentCaptor<Secret> c = reconcileCa(context, tlsCertificates);
        Map<String, String> data = c.getValue().getData();
        assertEquals(data.keySet().toString(), 2, data.size());
        assertEquals(initialSecret.getData().get("cluster-ca.crt"), data.get("cluster-ca.crt"));
        assertEquals(initialSecret.getData().get("cluster-ca.key"), data.get("cluster-ca.key"));
    }

    @Test
    public void expiredCertsNotRemovedManual(TestContext context) throws IOException {
        TlsCertificates tlsCertificates = new TlsCertificatesBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(false)
                .build();
        Secret initialSecret = initialSecret(tlsCertificates);
        initialSecret.getData().put("cluster-ca-2018-07-01T09:00:00.crt", "whatever");
        initialSecret.getData().put("cluster-ca-2018-07-01T09:00:00.key", "whatever");
        secrets.add(initialSecret);
        ArgumentCaptor<Secret> c = reconcileCa(context, tlsCertificates);
        Map<String, String> data = c.getValue().getData();
        assertEquals(initialSecret.getData(), data);
    }
}
