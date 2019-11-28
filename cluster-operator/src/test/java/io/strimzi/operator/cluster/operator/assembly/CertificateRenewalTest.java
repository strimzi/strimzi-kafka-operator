/*
 * Copyright Strimzi authors.
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
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.Ca.CA_CRT;
import static io.strimzi.operator.cluster.model.Ca.CA_KEY;
import static io.strimzi.operator.cluster.model.Ca.CA_STORE;
import static io.strimzi.operator.cluster.model.Ca.CA_STORE_PASSWORD;
import static io.strimzi.test.TestUtils.set;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class CertificateRenewalTest {

    public static final String NAMESPACE = "test";
    public static final String NAME = "my-kafka";
    private String clusterCaStorePassword = "123456";
    private Vertx vertx = Vertx.vertx();
    private OpenSslCertManager certManager = new OpenSslCertManager();
    private PasswordGenerator passwordGenerator = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");
    private List<Secret> secrets = new ArrayList();

    @BeforeEach
    public void clearSecrets() {
        secrets = new ArrayList();
    }

    private ArgumentCaptor<Secret> reconcileCa(VertxTestContext context, CertificateAuthority clusterCa, CertificateAuthority clientsCa) throws InterruptedException {
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

        return reconcileCa(context, kafka, () -> new Date());
    }

    private ArgumentCaptor<Secret> reconcileCa(VertxTestContext context, Kafka kafka, Supplier<Date> dateSupplier) throws InterruptedException {
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

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_9), certManager, passwordGenerator,
                new ResourceOperatorSupplier(null, null, null,
                        null, null, secretOps, null, null, null, null, null, null,
                        null, null, null, null, null, null, null, null, null, null, null, null, null),
                ResourceUtils.dummyClusterOperatorConfig(1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        AtomicReference<Throwable> error = new AtomicReference<>();
        Checkpoint async = context.checkpoint();
        op.new ReconciliationState(reconciliation, kafka).reconcileCas(dateSupplier).setHandler(ar -> {
            error.set(ar.cause());
            async.flag();
        });
        if (!context.awaitCompletion(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

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

    private CertAndKey generateCa(OpenSslCertManager certManager, CertificateAuthority certificateAuthority, String commonName)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        File clusterCaKeyFile = File.createTempFile("tls", "cluster-ca-key");
        File clusterCaCertFile = File.createTempFile("tls", "cluster-ca-cert");
        File clusterCaStoreFile = File.createTempFile("tls", "cluster-ca-store");
        try {
            Subject sbj = new Subject();
            sbj.setOrganizationName("io.strimzi");
            sbj.setCommonName(commonName);

            certManager.generateSelfSignedCert(clusterCaKeyFile, clusterCaCertFile, sbj, ModelUtils.getCertificateValidity(certificateAuthority));
            certManager.addCertToTrustStore(clusterCaCertFile, CA_CRT, clusterCaStoreFile, clusterCaStorePassword);
            return new CertAndKey(
                    Files.readAllBytes(clusterCaKeyFile.toPath()),
                    Files.readAllBytes(clusterCaCertFile.toPath()),
                    Files.readAllBytes(clusterCaStoreFile.toPath()),
                    null,
                    clusterCaStorePassword);
        } finally {
            clusterCaKeyFile.delete();
            clusterCaCertFile.delete();
            clusterCaStoreFile.delete();
        }
    }

    private List<Secret> initialClusterCaSecrets(CertificateAuthority certificateAuthority)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        return initialCaSecrets(certificateAuthority, "cluster-ca",
                AbstractModel.clusterCaKeySecretName(NAME),
                AbstractModel.clusterCaCertSecretName(NAME));
    }

    private List<Secret> initialClientsCaSecrets(CertificateAuthority certificateAuthority)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        return initialCaSecrets(certificateAuthority, "clients-ca",
                KafkaCluster.clientsCaKeySecretName(NAME),
                KafkaCluster.clientsCaCertSecretName(NAME));
    }

    private List<Secret> initialCaSecrets(CertificateAuthority certificateAuthority, String commonName,
                                          String caKeySecretName, String caCertSecretName)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertAndKey result = generateCa(certManager, certificateAuthority, commonName);
        List<Secret> secrets = new ArrayList<>();
        secrets.add(
                ResourceUtils.createInitialCaKeySecret(NAMESPACE, NAME, caKeySecretName, result.keyAsBase64String())
        );
        secrets.add(
                ResourceUtils.createInitialCaCertSecret(NAMESPACE, NAME, caCertSecretName,
                        result.certAsBase64String(), result.trustStoreAsBase64String(), result.storePasswordAsBase64String())
        );
        return secrets;
    }

    private boolean isCertInTrustStore(String alias, Map<String, String> data)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(new ByteArrayInputStream(
                Base64.getDecoder().decode(data.get(CA_STORE))),
                new String(Base64.getDecoder().decode(data.get(CA_STORE_PASSWORD)), StandardCharsets.US_ASCII).toCharArray()
        );
        return trustStore.isCertificateEntry(alias);
    }

    private X509Certificate getCertificateFromTrustStore(String alias, Map<String, String> data)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(new ByteArrayInputStream(
                Base64.getDecoder().decode(data.get(CA_STORE))),
                new String(Base64.getDecoder().decode(data.get(CA_STORE_PASSWORD)), StandardCharsets.US_ASCII).toCharArray()
        );
        return (X509Certificate) trustStore.getCertificate(alias);
    }

    @Test
    public void certsGetGeneratedInitiallyAuto(VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, InterruptedException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();
        secrets.clear();
        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertThat(c.getAllValues().size(), is(4));

        assertThat(c.getAllValues().get(0).getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(isCertInTrustStore(CA_CRT, c.getAllValues().get(0).getData()), is(true));
        assertThat(c.getAllValues().get(1).getData().keySet(), is(singleton(CA_KEY)));
        assertThat(c.getAllValues().get(2).getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(isCertInTrustStore(CA_CRT, c.getAllValues().get(2).getData()), is(true));
        assertThat(c.getAllValues().get(3).getData().keySet(), is(singleton(CA_KEY)));
    }

    @Test
    public void failsWhenCustomCertsAreMissing(VertxTestContext context) throws InterruptedException {
        assertThrows(InvalidConfigurationException.class, () -> {
            CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                    .withValidityDays(100)
                    .withRenewalDays(10)
                    .withGenerateCertificateAuthority(false)
                    .build();
            secrets.clear();
            reconcileCa(context, certificateAuthority, certificateAuthority);
        });
    }

    @Test
    public void noCertsGetGeneratedOutsideRenewalPeriodAuto(VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, InterruptedException {
        noCertsGetGeneratedOutsideRenewalPeriod(context, true);
    }

    private void noCertsGetGeneratedOutsideRenewalPeriod(VertxTestContext context, boolean generateCertificateAuthority)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, InterruptedException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(generateCertificateAuthority)
                .build();
        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClientsCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()), is(true));
        assertThat(initialClientsCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClientsCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);
        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);

        assertThat(c.getAllValues().get(0).getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(c.getAllValues().get(0).getData().get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
        assertThat(x509Certificate(initialClusterCaCertSecret.getData().get(CA_CRT))
                .equals(getCertificateFromTrustStore(CA_CRT, c.getAllValues().get(0).getData())), is(true));

        assertThat(c.getAllValues().get(1).getData().keySet(), is(set(CA_KEY)));
        assertThat(c.getAllValues().get(1).getData().get(CA_KEY), is(initialClusterCaKeySecret.getData().get(CA_KEY)));

        assertThat(c.getAllValues().get(2).getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(c.getAllValues().get(2).getData().get(CA_CRT), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
        assertThat(x509Certificate(initialClientsCaCertSecret.getData().get(CA_CRT))
                .equals(getCertificateFromTrustStore(CA_CRT, c.getAllValues().get(2).getData())), is(true));

        assertThat(c.getAllValues().get(3).getData().keySet(), is(set(CA_KEY)));
        assertThat(c.getAllValues().get(3).getData().get(CA_KEY), is(initialClientsCaKeySecret.getData().get(CA_KEY)));
    }

    @Test
    public void generateTruststoreFromOldSecrets(VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, InterruptedException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        // remove truststore and password to simulate Secrets coming from an older version
        initialClusterCaCertSecret.getData().remove(CA_STORE);
        initialClusterCaCertSecret.getData().remove(CA_STORE_PASSWORD);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        // remove truststore and password to simulate Secrets coming from an older version
        initialClientsCaCertSecret.getData().remove(CA_STORE);
        initialClientsCaCertSecret.getData().remove(CA_STORE_PASSWORD);

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertThat(c.getAllValues().size(), is(4));

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertThat(clusterCaCertData.keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        X509Certificate newX509ClusterCaCertStore = getCertificateFromTrustStore(CA_CRT, clusterCaCertData);
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
        String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClusterCaCert, is(notNullValue()));
        assertThat(newClusterCaCertStore, is(notNullValue()));
        assertThat(newClusterCaCertStorePassword, is(notNullValue()));
        assertThat(newClusterCaCert, is(initialClusterCaCertSecret.getData().get(CA_CRT)));
        assertThat(newX509ClusterCaCertStore.equals(x509Certificate(newClusterCaCert)), is(true));

        Map<String, String> clusterCaKeyData = c.getAllValues().get(1).getData();
        assertThat(clusterCaKeyData.keySet(), is(singleton(CA_KEY)));
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertThat(newClusterCaKey, is(notNullValue()));
        assertThat(newClusterCaKey, is(initialClusterCaKeySecret.getData().get(CA_KEY)));

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertThat(clientsCaCertData.keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        X509Certificate newX509ClientsCaCertStore = getCertificateFromTrustStore(CA_CRT, clientsCaCertData);
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
        String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClientsCaCert, is(notNullValue()));
        assertThat(newClientsCaCertStore, is(notNullValue()));
        assertThat(newClientsCaCertStorePassword, is(notNullValue()));
        assertThat(newClientsCaCert, is(initialClientsCaCertSecret.getData().get(CA_CRT)));
        assertThat(newX509ClientsCaCertStore.equals(x509Certificate(newClientsCaCert)), is(true));

        Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
        assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertThat(newClientsCaKey, is(notNullValue()));
        assertThat(newClientsCaKey, is(initialClientsCaKeySecret.getData().get(CA_KEY)));
    }

    @Test
    public void newCertsGetGeneratedWhenInRenewalPeriodAuto(VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, InterruptedException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .build();
        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClientsCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()), is(true));
        assertThat(initialClientsCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClientsCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertThat(c.getAllValues().size(), is(4));

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertThat(clusterCaCertData.keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        X509Certificate newX509ClusterCaCertStore = getCertificateFromTrustStore(CA_CRT, clusterCaCertData);
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
        String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClusterCaCert, is(notNullValue()));
        assertThat(newClusterCaCertStore, is(notNullValue()));
        assertThat(newClusterCaCertStorePassword, is(notNullValue()));
        assertThat(newClusterCaCert, is(not(initialClusterCaCertSecret.getData().get(CA_CRT))));
        assertThat(newClusterCaCertStore, is(not(initialClusterCaCertSecret.getData().get(CA_STORE))));
        assertThat(newClusterCaCertStorePassword, is(not(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD))));
        assertThat(newX509ClusterCaCertStore.equals(x509Certificate(newClusterCaCert)), is(true));

        Map<String, String> clusterCaKeyData = c.getAllValues().get(1).getData();
        assertThat(clusterCaKeyData.keySet(), is(singleton(CA_KEY)));
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertThat(newClusterCaKey, is(notNullValue()));
        assertThat(newClusterCaKey, is(initialClusterCaKeySecret.getData().get(CA_KEY)));

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertThat(clientsCaCertData.keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        X509Certificate newX509ClientsCaCertStore = getCertificateFromTrustStore(CA_CRT, clientsCaCertData);
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
        String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClientsCaCert, is(notNullValue()));
        assertThat(newClientsCaCertStore, is(notNullValue()));
        assertThat(newClientsCaCertStorePassword, is(notNullValue()));
        assertThat(newClientsCaCert, is(not(initialClientsCaCertSecret.getData().get(CA_CRT))));
        assertThat(newClientsCaCertStore, is(not(initialClientsCaCertSecret.getData().get(CA_STORE))));
        assertThat(newClientsCaCertStorePassword, is(not(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD))));
        assertThat(newX509ClientsCaCertStore.equals(x509Certificate(newClientsCaCert)), is(true));

        Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
        assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertThat(newClientsCaKey, is(notNullValue()));
        assertThat(newClientsCaKey, is(initialClientsCaKeySecret.getData().get(CA_KEY)));
    }

    @Test
    public void newCertsGetGeneratedWhenInRenewalPeriodAutoOutsideOfMaintenanceWindow(VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, InterruptedException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .build();

        Kafka kafka = new KafkaBuilder()
                .editOrNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClientsCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()), is(true));
        assertThat(initialClientsCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClientsCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, kafka, () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 00, 0).atZone(ZoneId.of("GMT")).toInstant()));
        assertThat(c.getAllValues().size(), is(4));

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertThat(clusterCaCertData.keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        X509Certificate newX509ClusterCaCertStore = getCertificateFromTrustStore(CA_CRT, clusterCaCertData);
        assertThat(c.getAllValues().get(0).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
        String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClusterCaCert, is(notNullValue()));
        assertThat(newClusterCaCertStore, is(notNullValue()));
        assertThat(newClusterCaCertStorePassword, is(notNullValue()));
        assertThat(newClusterCaCert, is(initialClusterCaCertSecret.getData().get(CA_CRT)));
        assertThat(newClusterCaCertStore, is(initialClusterCaCertSecret.getData().get(CA_STORE)));
        assertThat(newClusterCaCertStorePassword, is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
        assertThat(newX509ClusterCaCertStore.equals(x509Certificate(newClusterCaCert)), is(true));

        Map<String, String> clusterCaKeyData = c.getAllValues().get(1).getData();
        assertThat(clusterCaKeyData.keySet(), is(singleton(CA_KEY)));
        assertThat(c.getAllValues().get(1).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertThat(newClusterCaKey, is(notNullValue()));
        assertThat(newClusterCaKey, is(initialClusterCaKeySecret.getData().get(CA_KEY)));

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertThat(clientsCaCertData.keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        X509Certificate newX509ClientsCaCertStore = getCertificateFromTrustStore(CA_CRT, clientsCaCertData);
        assertThat(c.getAllValues().get(2).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
        String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClientsCaCert, is(notNullValue()));
        assertThat(newClientsCaCertStore, is(notNullValue()));
        assertThat(newClientsCaCertStorePassword, is(notNullValue()));
        assertThat(newClientsCaCert, is(initialClientsCaCertSecret.getData().get(CA_CRT)));
        assertThat(newClientsCaCertStore, is(initialClientsCaCertSecret.getData().get(CA_STORE)));
        assertThat(newClientsCaCertStorePassword, is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
        assertThat(newX509ClientsCaCertStore.equals(x509Certificate(newClientsCaCert)), is(true));

        Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
        assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
        assertThat(c.getAllValues().get(3).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertThat(newClientsCaKey, is(notNullValue()));
        assertThat(newClientsCaKey, is(initialClientsCaKeySecret.getData().get(CA_KEY)));
    }

    @Test
    public void newCertsGetGeneratedWhenInRenewalPeriodAutoWithinMaintenanceWindow(VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, InterruptedException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .build();

        Kafka kafka = new KafkaBuilder()
                .editOrNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClientsCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()), is(true));
        assertThat(initialClientsCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClientsCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, kafka, () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 12, 0).atZone(ZoneId.of("GMT")).toInstant()));
        assertThat(c.getAllValues().size(), is(4));

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertThat(clusterCaCertData.keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        X509Certificate newX509ClusterCaCertStore = getCertificateFromTrustStore(CA_CRT, clusterCaCertData);
        assertThat(c.getAllValues().get(0).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
        String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClusterCaCert, is(notNullValue()));
        assertThat(newClusterCaCertStore, is(notNullValue()));
        assertThat(newClusterCaCertStorePassword, is(notNullValue()));
        assertThat(newClusterCaCert, is(not(initialClusterCaCertSecret.getData().get(CA_CRT))));
        assertThat(newClusterCaCertStore, is(not(initialClusterCaCertSecret.getData().get(CA_STORE))));
        assertThat(newClusterCaCertStorePassword, is(not(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD))));
        assertThat(newX509ClusterCaCertStore.equals(x509Certificate(newClusterCaCert)), is(true));

        Map<String, String> clusterCaKeyData = c.getAllValues().get(1).getData();
        assertThat(clusterCaKeyData.keySet(), is(singleton(CA_KEY)));
        assertThat(c.getAllValues().get(1).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertThat(newClusterCaKey, is(notNullValue()));
        assertThat(newClusterCaKey, is(initialClusterCaKeySecret.getData().get(CA_KEY)));

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertThat(clientsCaCertData.keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        X509Certificate newX509ClientsCaCertStore = getCertificateFromTrustStore(CA_CRT, clientsCaCertData);
        assertThat(c.getAllValues().get(2).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
        String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClientsCaCert, is(notNullValue()));
        assertThat(newClientsCaCertStore, is(notNullValue()));
        assertThat(newClientsCaCertStorePassword, is(notNullValue()));
        assertThat(newClientsCaCert, is(not(initialClientsCaCertSecret.getData().get(CA_CRT))));
        assertThat(newClientsCaCertStore, is(not(initialClientsCaCertSecret.getData().get(CA_STORE))));
        assertThat(newClientsCaCertStorePassword, is(not(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD))));
        assertThat(newX509ClientsCaCertStore.equals(x509Certificate(newClientsCaCert)), is(true));

        Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
        assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
        assertThat(c.getAllValues().get(3).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertThat(newClientsCaKey, is(notNullValue()));
        assertThat(newClientsCaKey, is(initialClientsCaKeySecret.getData().get(CA_KEY)));
    }

    @Test
    public void newKeyGetGeneratedWhenInRenewalPeriodAuto(VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, InterruptedException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClientsCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()), is(true));
        assertThat(initialClientsCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClientsCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertThat(c.getAllValues().size(), is(4));

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertThat(clusterCaCertData.size(), is(4));
        X509Certificate newX509ClusterCaCertStore = getCertificateFromTrustStore(CA_CRT, clusterCaCertData);
        String oldClusterCaCertKey = clusterCaCertData.keySet()
                                                .stream()
                                                .filter(alias -> alias.startsWith("ca-"))
                                                .findAny()
                                                .get();
        X509Certificate oldX509ClusterCaCertStore = getCertificateFromTrustStore(oldClusterCaCertKey, clusterCaCertData);
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
        String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClusterCaCert, is(not(initialClusterCaCertSecret.getData().get(CA_CRT))));
        assertThat(newClusterCaCertStore, is(not(initialClusterCaCertSecret.getData().get(CA_STORE))));
        assertThat(newClusterCaCertStorePassword, is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
        assertThat(newX509ClusterCaCertStore.equals(x509Certificate(newClusterCaCert)), is(true));
        Map.Entry oldClusterCaCert = clusterCaCertData.entrySet().iterator().next();
        assertThat(oldClusterCaCert.getValue(), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
        assertThat(oldX509ClusterCaCertStore.equals(x509Certificate(String.valueOf(oldClusterCaCert.getValue()))), is(true));
        assertThat(x509Certificate(newClusterCaCert).getSubjectDN().getName(), is("CN=cluster-ca v1, O=io.strimzi"));

        Secret clusterCaKeySecret = c.getAllValues().get(1);
        assertThat(clusterCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("1"));
        Map<String, String> clusterCaKeyData = clusterCaKeySecret.getData();
        assertThat(clusterCaKeyData.keySet(), is(singleton(CA_KEY)));
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertThat(newClusterCaKey, is(notNullValue()));
        assertThat(newClusterCaKey, is(not(initialClusterCaKeySecret.getData().get(CA_KEY))));

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertThat(clientsCaCertData.size(), is(4));
        X509Certificate newX509ClientsCaCertStore = getCertificateFromTrustStore(CA_CRT, clientsCaCertData);
        String oldClientsCaCertKey = clientsCaCertData.keySet()
                                                    .stream()
                                                    .filter(alias -> alias.startsWith("ca-"))
                                                    .findAny()
                                                    .get();
        X509Certificate oldX509ClientsCaCertStore = getCertificateFromTrustStore(oldClientsCaCertKey, clientsCaCertData);
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
        String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClientsCaCert, is(not(initialClientsCaCertSecret.getData().get(CA_CRT))));
        assertThat(newClientsCaCertStore, is(not(initialClientsCaCertSecret.getData().get(CA_STORE))));
        assertThat(newClientsCaCertStorePassword, is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
        assertThat(newX509ClientsCaCertStore.equals(x509Certificate(newClientsCaCert)), is(true));
        Map.Entry oldClientsCaCert = clientsCaCertData.entrySet().iterator().next();
        assertThat(oldClientsCaCert.getValue(), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
        assertThat(oldX509ClientsCaCertStore.equals(x509Certificate(String.valueOf(oldClientsCaCert.getValue()))), is(true));
        assertThat(x509Certificate(newClientsCaCert).getSubjectDN().getName(), is("CN=clients-ca v1, O=io.strimzi"));

        Secret clientsCaKeySecret = c.getAllValues().get(3);
        assertThat(clientsCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("1"));
        Map<String, String> clientsCaKeyData = clientsCaKeySecret.getData();
        assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertThat(newClientsCaKey, is(notNullValue()));
        assertThat(newClientsCaKey, is(not(initialClientsCaKeySecret.getData().get(CA_KEY))));
    }

    @Test
    public void newKeyGetGeneratedWhenInRenewalPeriodAutoOutsideOfTimeWindow(VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, InterruptedException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                .build();

        Kafka kafka = new KafkaBuilder()
                .editOrNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClientsCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()), is(true));
        assertThat(initialClientsCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClientsCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, kafka, () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 0, 0).atZone(ZoneId.of("GMT")).toInstant()));
        assertThat(c.getAllValues().size(), is(4));

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertThat(c.getAllValues().get(0).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
        assertThat(clusterCaCertData.size(), is(3));
        X509Certificate newX509ClusterCaCertStore = getCertificateFromTrustStore(CA_CRT, clusterCaCertData);
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
        String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClusterCaCert, is(initialClusterCaCertSecret.getData().get(CA_CRT)));
        assertThat(newClusterCaCertStore, is(initialClusterCaCertSecret.getData().get(CA_STORE)));
        assertThat(newClusterCaCertStorePassword, is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
        assertThat(newX509ClusterCaCertStore.equals(x509Certificate(newClusterCaCert)), is(true));
        assertThat(x509Certificate(newClusterCaCert).getSubjectDN().getName(), is("CN=cluster-ca, O=io.strimzi"));

        Secret clusterCaKeySecret = c.getAllValues().get(1);
        assertThat(clusterCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
        Map<String, String> clusterCaKeyData = clusterCaKeySecret.getData();
        assertThat(clusterCaKeyData.keySet(), is(singleton(CA_KEY)));
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertThat(newClusterCaKey, is(notNullValue()));
        assertThat(newClusterCaKey, is(initialClusterCaKeySecret.getData().get(CA_KEY)));

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertThat(c.getAllValues().get(2).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("0"));
        assertThat(clientsCaCertData.size(), is(3));
        X509Certificate newX509ClientsCaCertStore = getCertificateFromTrustStore(CA_CRT, clientsCaCertData);
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
        String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClientsCaCert, is(initialClientsCaCertSecret.getData().get(CA_CRT)));
        assertThat(newClientsCaCertStore, is(initialClientsCaCertSecret.getData().get(CA_STORE)));
        assertThat(newClientsCaCertStorePassword, is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
        assertThat(newX509ClientsCaCertStore.equals(x509Certificate(newClientsCaCert)), is(true));
        assertThat(x509Certificate(newClientsCaCert).getSubjectDN().getName(), is("CN=clients-ca, O=io.strimzi"));

        Secret clientsCaKeySecret = c.getAllValues().get(3);
        assertThat(clientsCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
        Map<String, String> clientsCaKeyData = clientsCaKeySecret.getData();
        assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertThat(newClientsCaKey, is(notNullValue()));
        assertThat(newClientsCaKey, is(initialClientsCaKeySecret.getData().get(CA_KEY)));
    }

    @Test
    public void newKeyGetGeneratedWhenInRenewalPeriodAutoWithinTimeWindow(VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, InterruptedException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(true)
                .withCertificateExpirationPolicy(CertificateExpirationPolicy.REPLACE_KEY)
                .build();

        Kafka kafka = new KafkaBuilder()
                .editOrNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClientsCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()), is(true));
        assertThat(initialClientsCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClientsCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, kafka, () -> Date.from(LocalDateTime.of(2018, 11, 26, 9, 12, 0).atZone(ZoneId.of("GMT")).toInstant()));
        assertThat(c.getAllValues().size(), is(4));

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertThat(c.getAllValues().get(0).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        assertThat(clusterCaCertData.size(), is(4));
        X509Certificate newX509ClusterCaCertStore = getCertificateFromTrustStore(CA_CRT, clusterCaCertData);
        String oldClusterCaCertKey = clusterCaCertData.keySet()
                                                .stream()
                                                .filter(alias -> alias.startsWith("ca-"))
                                                .findAny()
                                                .get();
        X509Certificate oldX509ClusterCaCertStore = getCertificateFromTrustStore(oldClusterCaCertKey, clusterCaCertData);
        String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
        String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
        String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClusterCaCert, is(not(initialClusterCaCertSecret.getData().get(CA_CRT))));
        assertThat(newClusterCaCertStore, is(not(initialClusterCaCertSecret.getData().get(CA_STORE))));
        assertThat(newClusterCaCertStorePassword, is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
        assertThat(newX509ClusterCaCertStore.equals(x509Certificate(newClusterCaCert)), is(true));
        Map.Entry oldClusterCaCert = clusterCaCertData.entrySet().iterator().next();
        assertThat(oldClusterCaCert.getValue(), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
        assertThat(oldX509ClusterCaCertStore.equals(x509Certificate(String.valueOf(oldClusterCaCert.getValue()))), is(true));
        assertThat(x509Certificate(newClusterCaCert).getSubjectDN().getName(), is("CN=cluster-ca v1, O=io.strimzi"));

        Secret clusterCaKeySecret = c.getAllValues().get(1);
        assertThat(clusterCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("1"));
        Map<String, String> clusterCaKeyData = clusterCaKeySecret.getData();
        assertThat(clusterCaKeyData.keySet(), is(singleton(CA_KEY)));
        String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
        assertThat(newClusterCaKey, is(notNullValue()));
        assertThat(newClusterCaKey, is(not(initialClusterCaKeySecret.getData().get(CA_KEY))));

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertThat(c.getAllValues().get(2).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION), is("1"));
        assertThat(clientsCaCertData.size(), is(4));
        X509Certificate newX509ClientsCaCertStore = getCertificateFromTrustStore(CA_CRT, clientsCaCertData);
        String oldClientsCaCertKey = clientsCaCertData.keySet()
                                                    .stream()
                                                    .filter(alias -> alias.startsWith("ca-"))
                                                    .findAny()
                                                    .get();
        X509Certificate oldX509ClientsCaCertStore = getCertificateFromTrustStore(oldClientsCaCertKey, clientsCaCertData);
        String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
        String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
        String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
        assertThat(newClientsCaCert, is(not(initialClientsCaCertSecret.getData().get(CA_CRT))));
        assertThat(newClientsCaCertStore, is(not(initialClientsCaCertSecret.getData().get(CA_STORE))));
        assertThat(newClientsCaCertStorePassword, is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
        assertThat(newX509ClientsCaCertStore.equals(x509Certificate(newClientsCaCert)), is(true));
        Map.Entry oldClientsCaCert = clientsCaCertData.entrySet().iterator().next();
        assertThat(oldClientsCaCert.getValue(), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
        assertThat(oldX509ClientsCaCertStore.equals(x509Certificate(String.valueOf(oldClientsCaCert.getValue()))), is(true));
        assertThat(x509Certificate(newClientsCaCert).getSubjectDN().getName(), is("CN=clients-ca v1, O=io.strimzi"));

        Secret clientsCaKeySecret = c.getAllValues().get(3);
        assertThat(clientsCaKeySecret.getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("1"));
        Map<String, String> clientsCaKeyData = clientsCaKeySecret.getData();
        assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
        String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
        assertThat(newClientsCaKey, is(notNullValue()));
        assertThat(newClientsCaKey, is(not(initialClientsCaKeySecret.getData().get(CA_KEY))));
    }

    private X509Certificate x509Certificate(String newClusterCaCert) throws CertificateException {
        return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(newClusterCaCert)));
    }

    @Test
    public void expiredCertsGetRemovedAuto(VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, InterruptedException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));

        // add an expired certificate to the secret ...
        initialClusterCaCertSecret.getData().put("ca-2018-07-01T09-00-00.crt",
                Base64.getEncoder().encodeToString(
                        TestUtils.readResource(getClass(), "cluster-ca.crt").getBytes(StandardCharsets.UTF_8)));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));
        // ... and to the related truststore
        File certFile = File.createTempFile("tls", "-cert");
        Files.write(certFile.toPath(), Base64.getDecoder().decode(initialClusterCaCertSecret.getData().get("ca-2018-07-01T09-00-00.crt")));
        File trustStoreFile = File.createTempFile("tls", "-truststore");
        Files.write(trustStoreFile.toPath(), Base64.getDecoder().decode(initialClusterCaCertSecret.getData().get(CA_STORE)));
        String trustStorePassword = new String(Base64.getDecoder().decode(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)), StandardCharsets.US_ASCII);
        certManager.addCertToTrustStore(certFile, "ca-2018-07-01T09-00-00.crt", trustStoreFile, trustStorePassword);
        initialClusterCaCertSecret.getData().put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile.toPath())));
        assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", initialClusterCaCertSecret.getData()), is(true));
        trustStoreFile.delete();
        certFile.delete();

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClientsCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()), is(true));

        // add an expired certificate to the secret ...
        initialClientsCaCertSecret.getData().put("ca-2018-07-01T09-00-00.crt",
                Base64.getEncoder().encodeToString(
                TestUtils.readResource(getClass(), "clients-ca.crt").getBytes(StandardCharsets.UTF_8)));
        assertThat(initialClientsCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClientsCaKeySecret.getData().get(CA_KEY), is(notNullValue()));
        // ... and to the related truststore
        certFile = File.createTempFile("tls", "-cert");
        Files.write(certFile.toPath(), Base64.getDecoder().decode(initialClientsCaCertSecret.getData().get("ca-2018-07-01T09-00-00.crt")));
        trustStoreFile = File.createTempFile("tls", "-truststore");
        Files.write(trustStoreFile.toPath(), Base64.getDecoder().decode(initialClientsCaCertSecret.getData().get(CA_STORE)));
        trustStorePassword = new String(Base64.getDecoder().decode(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)), StandardCharsets.US_ASCII);
        certManager.addCertToTrustStore(certFile, "ca-2018-07-01T09-00-00.crt", trustStoreFile, trustStorePassword);
        initialClientsCaCertSecret.getData().put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile.toPath())));
        assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", initialClientsCaCertSecret.getData()), is(true));
        trustStoreFile.delete();
        certFile.delete();

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertThat(c.getAllValues().size(), is(4));

        Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
        assertThat(clusterCaCertData.keySet().toString(), clusterCaCertData.size(), is(3));
        assertThat(clusterCaCertData.get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
        assertThat(clusterCaCertData.get(CA_STORE), is(initialClusterCaCertSecret.getData().get(CA_STORE)));
        assertThat(clusterCaCertData.get(CA_STORE_PASSWORD), is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
        assertThat(getCertificateFromTrustStore(CA_CRT, clusterCaCertData).equals(x509Certificate(clusterCaCertData.get(CA_CRT))), is(true));
        Map<String, String> clusterCaKeyData = c.getAllValues().get(1).getData();
        assertThat(clusterCaKeyData.get(CA_KEY), is(initialClusterCaKeySecret.getData().get(CA_KEY)));
        assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", clusterCaCertData), is(false));

        Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
        assertThat(clientsCaCertData.keySet().toString(), clientsCaCertData.size(), is(3));
        assertThat(clientsCaCertData.get(CA_CRT), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
        assertThat(clientsCaCertData.get(CA_STORE), is(initialClientsCaCertSecret.getData().get(CA_STORE)));
        assertThat(clientsCaCertData.get(CA_STORE_PASSWORD), is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
        assertThat(getCertificateFromTrustStore(CA_CRT, clientsCaCertData).equals(x509Certificate(clientsCaCertData.get(CA_CRT))), is(true));
        Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
        assertThat(clientsCaKeyData.get(CA_KEY), is(initialClientsCaKeySecret.getData().get(CA_KEY)));
        assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", clientsCaCertData), is(false));
    }

    @Test
    public void customCertsNotReconciled(VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, InterruptedException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(2)
                .withRenewalDays(3)
                .withGenerateCertificateAuthority(false)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClientsCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()), is(true));
        assertThat(initialClientsCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClientsCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        ArgumentCaptor<Secret> c = reconcileCa(context, certificateAuthority, certificateAuthority);
        assertThat(c.getAllValues().size(), is(0));
    }
}