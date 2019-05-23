/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.operator.KubernetesVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
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

    private ArgumentCaptor<Secret> reconcileCa(TestContext context, CertificateAuthority clusterCa, CertificateAuthority clientsCa) {
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
        when(secretOps.reconcile(eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), c.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.noop(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), c.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.noop(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(KafkaCluster.clientsCaCertSecretName(NAME)), c.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.noop(i.getArgument(0))));
        when(secretOps.reconcile(eq(NAMESPACE), eq(KafkaCluster.clientsCaKeySecretName(NAME)), c.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.noop(i.getArgument(0))));

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_9), certManager,
                new ResourceOperatorSupplier(null, null, null,
                        null, null, secretOps, null, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null, null, null),
                ResourceUtils.dummyClusterOperatorConfig(1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", ResourceType.KAFKA, NAMESPACE, NAME);

        Kafka kafka = new KafkaBuilder()
                .editOrNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withClusterCa(clusterCa)
                    .withClientsCa(clientsCa)
                .endSpec()
            .build();

        AtomicReference<Throwable> error = new AtomicReference<>();
        Async async = context.async();
        op.new ReconciliationState(reconciliation, kafka).reconcileCas().setHandler(ar -> {
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

    private CertAndKey generateCa(OpenSslCertManager certManager, CertificateAuthority certificateAuthority, String commonName) throws IOException {
        File clusterCaKeyFile = File.createTempFile("tls", "cluster-ca-key");
        File clusterCaCertFile = File.createTempFile("tls", "cluster-ca-cert");
        try {
            Subject sbj = new Subject();
            sbj.setOrganizationName("io.strimzi");
            sbj.setCommonName(commonName);

            certManager.generateSelfSignedCert(clusterCaKeyFile, clusterCaCertFile, sbj, ModelUtils.getCertificateValidity(certificateAuthority));
            return new CertAndKey(Files.readAllBytes(clusterCaKeyFile.toPath()),
                    Files.readAllBytes(clusterCaCertFile.toPath()));
        } finally {
            clusterCaKeyFile.delete();
            clusterCaCertFile.delete();
        }
    }

    private Secret initialClusterCaCertSecret(CertificateAuthority certificateAuthority) throws IOException {
        String commonName = "cluster-ca";
        CertAndKey result = generateCa(certManager, certificateAuthority, commonName);
        return ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME,
                AbstractModel.clusterCaCertSecretName(NAME), result.certAsBase64String());
    }

    private Secret initialClusterCaKeySecret(CertificateAuthority certificateAuthority) throws IOException {
        String commonName = "cluster-ca";
        CertAndKey result = generateCa(certManager, certificateAuthority, commonName);
        return ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME,
                AbstractModel.clusterCaKeySecretName(NAME), result.keyAsBase64String());
    }

    private Secret initialClientsCaCertSecret(CertificateAuthority certificateAuthority) throws IOException {
        String commonName = "clients-ca";
        CertAndKey result = generateCa(certManager, certificateAuthority, commonName);
        return ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME,
                KafkaCluster.clientsCaCertSecretName(NAME), result.certAsBase64String());
    }

    private Secret initialClientsCaKeySecret(CertificateAuthority certificateAuthority) throws IOException {
        String commonName = "clients-ca";
        CertAndKey result = generateCa(certManager, certificateAuthority, commonName);
        return ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME,
                KafkaCluster.clientsCaKeySecretName(NAME), result.keyAsBase64String());
    }

    /***/
    @Test
    public void certsGetGeneratedInitiallyAuto(TestContext context) throws IOException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();
        secrets.clear();
        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertEquals(4, c.getAllValues().size());

        assertEquals(singleton(CA_CRT), c.getAllValues().get(0).getData().keySet());
        assertEquals(singleton(CA_KEY), c.getAllValues().get(1).getData().keySet());
        assertEquals(singleton(CA_CRT), c.getAllValues().get(2).getData().keySet());
        assertEquals(singleton(CA_KEY), c.getAllValues().get(3).getData().keySet());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void failsWhenCustomCertsAreMissing(TestContext context) throws IOException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(false)
                .build();
        secrets.clear();
        reconcileCa(context, certificateAuthority, certificateAuthority);
    }

    @Test
    public void noCertsGetGeneratedOutsideRenewalPeriodAuto(TestContext context) throws IOException {
        noCertsGetGeneratedOutsideRenewalPeriod(context, true);
    }

    private void noCertsGetGeneratedOutsideRenewalPeriod(TestContext context, boolean generateCertificateAuthority) throws IOException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(generateCertificateAuthority)
                .build();
        Secret initialClusterCaCertSecret = initialClusterCaCertSecret(certificateAuthority);
        Secret initialClusterCaKeySecret = initialClusterCaKeySecret(certificateAuthority);
        assertEquals(singleton(CA_CRT), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));

        Secret initialClientsCaCertSecret = initialClientsCaCertSecret(certificateAuthority);
        Secret initialClientsCaKeySecret = initialClientsCaKeySecret(certificateAuthority);
        assertEquals(singleton(CA_CRT), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);
        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);

        assertEquals(set(CA_CRT), c.getAllValues().get(0).getData().keySet());
        assertEquals(initialClusterCaCertSecret.getData().get(CA_CRT), c.getAllValues().get(0).getData().get(CA_CRT));

        assertEquals(set(CA_KEY), c.getAllValues().get(1).getData().keySet());
        assertEquals(initialClusterCaKeySecret.getData().get(CA_KEY), c.getAllValues().get(1).getData().get(CA_KEY));

        assertEquals(set(CA_CRT), c.getAllValues().get(2).getData().keySet());
        assertEquals(initialClientsCaCertSecret.getData().get(CA_CRT), c.getAllValues().get(2).getData().get(CA_CRT));

        assertEquals(set(CA_KEY), c.getAllValues().get(3).getData().keySet());
        assertEquals(initialClientsCaKeySecret.getData().get(CA_KEY), c.getAllValues().get(3).getData().get(CA_KEY));
    }

    @Test
    public void newCertsGetGeneratedWhenInRenewalPeriodAuto(TestContext context) throws IOException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .build();
        Secret initialClusterCaCertSecret = initialClusterCaCertSecret(certificateAuthority);
        Secret initialClusterCaKeySecret = initialClusterCaKeySecret(certificateAuthority);
        assertEquals(singleton(CA_CRT), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));

        Secret initialClientsCaCertSecret = initialClientsCaCertSecret(certificateAuthority);
        Secret initialClientsCaKeySecret = initialClientsCaKeySecret(certificateAuthority);
        assertEquals(singleton(CA_CRT), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertEquals(4, c.getAllValues().size());

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertEquals(singleton(CA_CRT), clusterCaCertData.keySet());
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        assertNotNull(newClusterCaCert);
        assertNotEquals(initialClusterCaCertSecret.getData().get(CA_CRT), newClusterCaCert);

        Map<String, String> clusterCaKeyData = c.getAllValues().get(1).getData();
        assertEquals(singleton(CA_KEY), clusterCaKeyData.keySet());
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertNotNull(newClusterCaKey);
        assertEquals(initialClusterCaKeySecret.getData().get(CA_KEY), newClusterCaKey);

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertEquals(singleton(CA_CRT), clientsCaCertData.keySet());
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        assertNotNull(newClientsCaCert);
        assertNotEquals(initialClientsCaCertSecret.getData().get(CA_CRT), newClientsCaCert);

        Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
        assertEquals(singleton(CA_KEY), clientsCaKeyData.keySet());
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertNotNull(newClientsCaKey);
        assertEquals(initialClientsCaKeySecret.getData().get(CA_KEY), newClientsCaKey);
    }

    @Test
    public void newKeyGetGeneratedWhenInRenewalPeriodAuto(TestContext context) throws IOException, CertificateException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                .build();
        Secret initialClusterCaCertSecret = initialClusterCaCertSecret(certificateAuthority);
        Secret initialClusterCaKeySecret = initialClusterCaKeySecret(certificateAuthority);
        assertEquals(singleton(CA_CRT), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));

        Secret initialClientsCaCertSecret = initialClientsCaCertSecret(certificateAuthority);
        Secret initialClientsCaKeySecret = initialClientsCaKeySecret(certificateAuthority);
        assertEquals(singleton(CA_CRT), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertEquals(4, c.getAllValues().size());

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertEquals(2, clusterCaCertData.size());
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        assertNotEquals(initialClusterCaCertSecret.getData().get(CA_CRT), newClusterCaCert);
        Map.Entry oldClusterCaCert = clusterCaCertData.entrySet().iterator().next();
        assertEquals(initialClusterCaCertSecret.getData().get(CA_CRT), oldClusterCaCert.getValue());
        assertEquals("CN=cluster-ca v1, O=io.strimzi", x509Certificate(newClusterCaCert).getSubjectDN().getName());

        Secret clusterCaKeySecret = c.getAllValues().get(1);
        assertEquals("1", clusterCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION));
        Map<String, String> clusterCaKeyData = clusterCaKeySecret.getData();
        assertEquals(singleton(CA_KEY), clusterCaKeyData.keySet());
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertNotNull(newClusterCaKey);
        assertNotEquals(initialClusterCaKeySecret.getData().get(CA_KEY), newClusterCaKey);

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertEquals(2, clientsCaCertData.size());
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        assertNotEquals(initialClientsCaCertSecret.getData().get(CA_CRT), newClientsCaCert);
        Map.Entry oldClientsCaCert = clientsCaCertData.entrySet().iterator().next();
        assertEquals(initialClientsCaCertSecret.getData().get(CA_CRT), oldClientsCaCert.getValue());
        assertEquals("CN=clients-ca v1, O=io.strimzi", x509Certificate(newClientsCaCert).getSubjectDN().getName());

        Secret clientsCaKeySecret = c.getAllValues().get(3);
        assertEquals("1", clientsCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION));
        Map<String, String> clientsCaKeyData = clientsCaKeySecret.getData();
        assertEquals(singleton(CA_KEY), clientsCaKeyData.keySet());
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertNotNull(newClientsCaKey);
        assertNotEquals(initialClientsCaKeySecret.getData().get(CA_KEY), newClientsCaKey);
    }

    private X509Certificate x509Certificate(String newClusterCaCert) throws CertificateException {
        return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(newClusterCaCert)));
    }

    @Test
    public void expiredCertsGetRemovedAuto(TestContext context) throws IOException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();
        Secret initialClusterCaCertSecret = initialClusterCaCertSecret(certificateAuthority);
        Secret initialClusterCaKeySecret = initialClusterCaKeySecret(certificateAuthority);
        assertEquals(singleton(CA_CRT), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));

        initialClusterCaCertSecret.getData().put("ca-2018-07-01T09-00-00.crt",
                Base64.getEncoder().encodeToString(
                        TestUtils.readResource(getClass(), "cluster-ca.crt").getBytes(StandardCharsets.UTF_8)));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));

        Secret initialClientsCaCertSecret = initialClientsCaCertSecret(certificateAuthority);
        Secret initialClientsCaKeySecret = initialClientsCaKeySecret(certificateAuthority);
        assertEquals(singleton(CA_CRT), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        initialClientsCaCertSecret.getData().put("ca-2018-07-01T09-00-00.crt",
                Base64.getEncoder().encodeToString(
                TestUtils.readResource(getClass(), "clients-ca.crt").getBytes(StandardCharsets.UTF_8)));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertEquals(4, c.getAllValues().size());

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertEquals(clusterCaCertData.keySet().toString(), 1, clusterCaCertData.size());
        assertEquals(initialClusterCaCertSecret.getData().get(CA_CRT), clusterCaCertData.get(CA_CRT));
        Map<String, String> clusterCaKeyData = c.getAllValues().get(1).getData();
        assertEquals(initialClusterCaKeySecret.getData().get(CA_KEY), clusterCaKeyData.get(CA_KEY));

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertEquals(clientsCaCertData.keySet().toString(), 1, clientsCaCertData.size());
        assertEquals(initialClientsCaCertSecret.getData().get(CA_CRT), clientsCaCertData.get(CA_CRT));
        Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
        assertEquals(initialClientsCaKeySecret.getData().get(CA_KEY), clientsCaKeyData.get(CA_KEY));
    }

    @Test
    public void customCertsNotReconciled(TestContext context) throws IOException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(false)
                .build();
        Secret initialClusterCaCertSecret = initialClusterCaCertSecret(certificateAuthority);
        Secret initialClusterCaKeySecret = initialClusterCaKeySecret(certificateAuthority);
        assertEquals(singleton(CA_CRT), initialClusterCaCertSecret.getData().keySet());
        assertNotNull(initialClusterCaCertSecret.getData().get(CA_CRT));
        assertEquals(singleton(CA_KEY), initialClusterCaKeySecret.getData().keySet());
        assertNotNull(initialClusterCaKeySecret.getData().get(CA_KEY));

        Secret initialClientsCaCertSecret = initialClientsCaCertSecret(certificateAuthority);
        Secret initialClientsCaKeySecret = initialClientsCaKeySecret(certificateAuthority);
        assertEquals(singleton(CA_CRT), initialClientsCaCertSecret.getData().keySet());
        assertNotNull(initialClientsCaCertSecret.getData().get(CA_CRT));
        assertEquals(singleton(CA_KEY), initialClientsCaKeySecret.getData().keySet());
        assertNotNull(initialClientsCaKeySecret.getData().get(CA_KEY));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertEquals(0, c.getAllValues().size());
    }
}
