/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.common.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.model.Ca.CA_CRT;
import static io.strimzi.operator.common.model.Ca.CA_KEY;
import static io.strimzi.operator.common.model.Ca.CA_STORE;
import static io.strimzi.operator.common.model.Ca.CA_STORE_PASSWORD;
import static io.strimzi.test.TestUtils.set;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
@ExtendWith(VertxExtension.class)
public class CaReconcilerTest {
    public static final String NAMESPACE = "test";
    public static final String NAME = "my-kafka";

    private final OpenSslCertManager certManager = new OpenSslCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");

    private List<Secret> secrets = new ArrayList<>();
    private WorkerExecutor sharedWorkerExecutor;

    @BeforeEach
    public void setup(Vertx vertx) {
        secrets = new ArrayList<>();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterEach
    public void teardown() {
        sharedWorkerExecutor.close();
    }

    private Future<ArgumentCaptor<Secret>> reconcileCa(Vertx vertx, CertificateAuthority clusterCa, CertificateAuthority clientsCa) {
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

        return reconcileCa(vertx, kafka, Clock.systemUTC());
    }

    private Future<ArgumentCaptor<Secret>> reconcileCa(Vertx vertx, Kafka kafka, Clock clock) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        SecretOperator secretOps = supplier.secretOperations;
        DeploymentOperator deploymentOps = supplier.deploymentOperations;
        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
        PodOperator podOps = supplier.podOperations;

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenAnswer(invocation -> {
            Map<String, String> requiredLabels = ((Labels) invocation.getArgument(1)).toMap();

            List<Secret> listedSecrets = secrets.stream().filter(s -> {
                Map<String, String> labels = s.getMetadata().getLabels();
                labels.keySet().retainAll(requiredLabels.keySet());
                return labels.equals(requiredLabels);
            }).collect(Collectors.toList());

            return Future.succeededFuture(listedSecrets);
        });
        ArgumentCaptor<Secret> c = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), c.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.noop(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), c.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.noop(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), c.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.noop(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), c.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.noop(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.secretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));

        when(deploymentOps.getAsync(eq(NAMESPACE), any())).thenReturn(Future.succeededFuture());

        when(spsOps.getAsync(eq(NAMESPACE), any())).thenReturn(Future.succeededFuture());
        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture());

        when(podOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Promise<ArgumentCaptor<Secret>> reconcileCasComplete = Promise.promise();

        new CaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, certManager, passwordGenerator)
                .reconcile(clock)
                .onComplete(ar -> {
                    // If succeeded return the argument captor object instead of the Reconciliation state
                    // This is for the purposes of testing
                    // If failed then return the throwable of the reconcileCas
                    if (ar.succeeded()) {
                        reconcileCasComplete.complete(c);
                    } else {
                        reconcileCasComplete.fail(ar.cause());
                    }
                });

        return reconcileCasComplete.future();
    }

    private CertAndKey generateCa(OpenSslCertManager certManager, CertificateAuthority certificateAuthority, String commonName)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        String clusterCaStorePassword = "123456";

        Path clusterCaKeyFile = Files.createTempFile("tls", "cluster-ca-key");
        clusterCaKeyFile.toFile().deleteOnExit();
        Path clusterCaCertFile = Files.createTempFile("tls", "cluster-ca-cert");
        clusterCaCertFile.toFile().deleteOnExit();
        Path clusterCaStoreFile = Files.createTempFile("tls", "cluster-ca-store");
        clusterCaStoreFile.toFile().deleteOnExit();
        
        Subject sbj = new Subject.Builder()
                .withOrganizationName("io.strimzi")
                .withCommonName(commonName).build();

        certManager.generateSelfSignedCert(clusterCaKeyFile.toFile(), clusterCaCertFile.toFile(), sbj, ModelUtils.getCertificateValidity(certificateAuthority));

        certManager.addCertToTrustStore(clusterCaCertFile.toFile(), CA_CRT, clusterCaStoreFile.toFile(), clusterCaStorePassword);
        return new CertAndKey(
                Files.readAllBytes(clusterCaKeyFile),
                Files.readAllBytes(clusterCaCertFile),
                Files.readAllBytes(clusterCaStoreFile),
                null,
                clusterCaStorePassword);
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
                KafkaResources.clientsCaKeySecretName(NAME),
                KafkaResources.clientsCaCertificateSecretName(NAME));
    }

    private List<Secret> initialCaSecrets(CertificateAuthority certificateAuthority, String commonName, String caKeySecretName, String caCertSecretName)
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

    private KeyStore getTrustStore(Map<String, String> data)
            throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(new ByteArrayInputStream(
                Util.decodeBytesFromBase64(data.get(CA_STORE))),
                Util.decodeFromBase64(data.get(CA_STORE_PASSWORD)).toCharArray()
        );
        return trustStore;
    }

    private boolean isCertInTrustStore(String alias, Map<String, String> data)
            throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        KeyStore trustStore = getTrustStore(data);
        return trustStore.isCertificateEntry(alias);
    }

    private X509Certificate getCertificateFromTrustStore(String alias, Map<String, String> data)
            throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        KeyStore trustStore = getTrustStore(data);
        return (X509Certificate) trustStore.getCertificate(alias);
    }


    @Test
    public void testReconcileCasGeneratesCertsInitially(Vertx vertx, VertxTestContext context) {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        // Delete secrets to emulate secrets not pre-existing
        secrets.clear();

        Checkpoint async = context.checkpoint();
        reconcileCa(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.succeeding(c -> context.verify(() -> {
                assertThat(c.getAllValues(), hasSize(4));

                assertThat(c.getAllValues().get(0).getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
                assertThat(isCertInTrustStore(CA_CRT, c.getAllValues().get(0).getData()), is(true));

                assertThat(c.getAllValues().get(1).getData().keySet(), is(singleton(CA_KEY)));

                assertThat(c.getAllValues().get(2).getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
                assertThat(isCertInTrustStore(CA_CRT, c.getAllValues().get(2).getData()), is(true));

                assertThat(c.getAllValues().get(3).getData().keySet(), is(singleton(CA_KEY)));

                async.flag();
            })));
    }

    @Test
    public void testReconcileCasWhenCustomCertsAreMissingThrows(Vertx vertx, VertxTestContext context) {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(false)
                .build();

        Checkpoint async = context.checkpoint();
        reconcileCa(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(InvalidResourceException.class));
                assertThat(e.getMessage(), is("Cluster CA should not be generated, but the secrets were not found."));
                async.flag();
            })));
    }

    @Test
    public void testReconcileCasNoCertsGetGeneratedOutsideRenewalPeriod(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        assertNoCertsGetGeneratedOutsideRenewalPeriod(vertx, context);
    }

    private void assertNoCertsGetGeneratedOutsideRenewalPeriod(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        Map<String, String> clusterCaCertData = initialClusterCaCertSecret.getData();
        assertThat(clusterCaCertData.keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(clusterCaCertData.get(CA_CRT), is(notNullValue()));
        assertThat(clusterCaCertData.get(CA_STORE), is(notNullValue()));
        assertThat(clusterCaCertData.get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));

        Map<String, String> clusterCaKeyData = initialClusterCaKeySecret.getData();
        assertThat(clusterCaKeyData.keySet(), is(singleton(CA_KEY)));
        assertThat(clusterCaKeyData.get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        Map<String, String> clientsCaCertData = initialClientsCaCertSecret.getData();
        assertThat(clientsCaCertData.keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(clientsCaCertData.get(CA_CRT), is(notNullValue()));
        assertThat(clientsCaCertData.get(CA_STORE), is(notNullValue()));
        assertThat(clientsCaCertData.get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()), is(true));

        Map<String, String> clientsCaKeyData = initialClientsCaKeySecret.getData();
        assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
        assertThat(clientsCaKeyData.get(CA_KEY), is(notNullValue()));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        Checkpoint async = context.checkpoint();

        reconcileCa(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.succeeding(c -> context.verify(() -> {

                assertThat(c.getAllValues().get(0).getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
                assertThat(c.getAllValues().get(0).getData().get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                assertThat(x509Certificate(initialClusterCaCertSecret.getData().get(CA_CRT)), is(getCertificateFromTrustStore(CA_CRT, c.getAllValues().get(0).getData())));

                assertThat(c.getAllValues().get(1).getData().keySet(), is(set(CA_KEY)));
                assertThat(c.getAllValues().get(1).getData().get(CA_KEY), is(initialClusterCaKeySecret.getData().get(CA_KEY)));

                assertThat(c.getAllValues().get(2).getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
                assertThat(c.getAllValues().get(2).getData().get(CA_CRT), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                assertThat(x509Certificate(initialClientsCaCertSecret.getData().get(CA_CRT)), is(getCertificateFromTrustStore(CA_CRT, c.getAllValues().get(2).getData())));

                assertThat(c.getAllValues().get(3).getData().keySet(), is(set(CA_KEY)));
                assertThat(c.getAllValues().get(3).getData().get(CA_KEY), is(initialClientsCaKeySecret.getData().get(CA_KEY)));
                async.flag();
            })));

    }

    @Test
    public void testGenerateTruststoreFromOldSecrets(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
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

        Checkpoint async = context.checkpoint();
        reconcileCa(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.succeeding(c -> context.verify(() -> {
                assertThat(c.getAllValues(), hasSize(4));

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
                assertThat(newX509ClusterCaCertStore, is(x509Certificate(newClusterCaCert)));

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
                assertThat(newX509ClientsCaCertStore, is(x509Certificate(newClientsCaCert)));

                Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
                assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
                String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
                assertThat(newClientsCaKey, is(notNullValue()));
                assertThat(newClientsCaKey, is(initialClientsCaKeySecret.getData().get(CA_KEY)));
                async.flag();
            })));
    }

    @Test
    public void testNewCertsGetGeneratedWhenInRenewalPeriodAuto(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
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

        Checkpoint async = context.checkpoint();
        reconcileCa(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.succeeding(c -> context.verify(() -> {
                assertThat(c.getAllValues(), hasSize(4));

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
                assertThat(newX509ClusterCaCertStore, is(x509Certificate(newClusterCaCert)));

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
                assertThat(newX509ClientsCaCertStore, is(x509Certificate(newClientsCaCert)));

                Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
                assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
                String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
                assertThat(newClientsCaKey, is(notNullValue()));
                assertThat(newClientsCaKey, is(initialClientsCaKeySecret.getData().get(CA_KEY)));

                async.flag();
            })));
    }

    @Test
    public void testNewCertsGetGeneratedWhenInRenewalPeriodAutoOutsideOfMaintenanceWindow(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
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

        Checkpoint async = context.checkpoint();
        reconcileCa(vertx, kafka, Clock.fixed(Instant.parse("2018-11-26T09:00:00Z"), Clock.systemUTC().getZone()))
            .onComplete(context.succeeding(c -> context.verify(() -> {
                assertThat(c.getAllValues(), hasSize(4));

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
                assertThat(newX509ClusterCaCertStore, is(x509Certificate(newClusterCaCert)));

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
                assertThat(newX509ClientsCaCertStore, is(x509Certificate(newClientsCaCert)));

                Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
                assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
                assertThat(c.getAllValues().get(3).getMetadata().getAnnotations().get(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION), is("0"));
                String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
                assertThat(newClientsCaKey, is(notNullValue()));
                assertThat(newClientsCaKey, is(initialClientsCaKeySecret.getData().get(CA_KEY)));

                async.flag();
            })));
    }

    @Test
    public void testNewCertsGetGeneratedWhenInRenewalPeriodAutoWithinMaintenanceWindow(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
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

        Checkpoint async = context.checkpoint();
        reconcileCa(vertx, kafka, Clock.fixed(Instant.parse("2018-11-26T10:12:00Z"), Clock.systemUTC().getZone()))
            .onComplete(context.succeeding(c -> context.verify(() -> {
                assertThat(c.getAllValues().size(), is(4));

                Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
                assertThat(clusterCaCertData.keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
                X509Certificate newX509ClusterCaCertStore = getCertificateFromTrustStore(CA_CRT, clusterCaCertData);
                assertThat(c.getAllValues().get(0).getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"));
                String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
                String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
                String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
                assertThat(newClusterCaCert, is(notNullValue()));
                assertThat(newClusterCaCertStore, is(notNullValue()));
                assertThat(newClusterCaCertStorePassword, is(notNullValue()));
                assertThat(newClusterCaCert, is(not(initialClusterCaCertSecret.getData().get(CA_CRT))));
                assertThat(newClusterCaCertStore, is(not(initialClusterCaCertSecret.getData().get(CA_STORE))));
                assertThat(newClusterCaCertStorePassword, is(not(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD))));
                assertThat(newX509ClusterCaCertStore, is(x509Certificate(newClusterCaCert)));

                Map<String, String> clusterCaKeyData = c.getAllValues().get(1).getData();
                assertThat(clusterCaKeyData.keySet(), is(singleton(CA_KEY)));
                assertThat(c.getAllValues().get(1).getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0"));
                String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
                assertThat(newClusterCaKey, is(notNullValue()));
                assertThat(newClusterCaKey, is(initialClusterCaKeySecret.getData().get(CA_KEY)));

                Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
                assertThat(clientsCaCertData.keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
                X509Certificate newX509ClientsCaCertStore = getCertificateFromTrustStore(CA_CRT, clientsCaCertData);
                assertThat(c.getAllValues().get(2).getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"));
                String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
                String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
                String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
                assertThat(newClientsCaCert, is(notNullValue()));
                assertThat(newClientsCaCertStore, is(notNullValue()));
                assertThat(newClientsCaCertStorePassword, is(notNullValue()));
                assertThat(newClientsCaCert, is(not(initialClientsCaCertSecret.getData().get(CA_CRT))));
                assertThat(newClientsCaCertStore, is(not(initialClientsCaCertSecret.getData().get(CA_STORE))));
                assertThat(newClientsCaCertStorePassword, is(not(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD))));
                assertThat(newX509ClientsCaCertStore, is(x509Certificate(newClientsCaCert)));

                Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
                assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
                assertThat(c.getAllValues().get(3).getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0"));
                String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
                assertThat(newClientsCaKey, is(notNullValue()));
                assertThat(newClientsCaKey, is(initialClientsCaKeySecret.getData().get(CA_KEY)));
                async.flag();
            })));
    }

    @Test
    public void testNewKeyGetGeneratedWhenInRenewalPeriodAuto(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
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

        Checkpoint async = context.checkpoint();
        reconcileCa(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.succeeding(c -> context.verify(() -> {
                assertThat(c.getAllValues(), hasSize(4));

                Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
                assertThat(clusterCaCertData, aMapWithSize(4));
                X509Certificate newX509ClusterCaCertStore = getCertificateFromTrustStore(CA_CRT, clusterCaCertData);
                String oldClusterCaCertKey = clusterCaCertData.keySet()
                        .stream()
                        .filter(alias -> alias.startsWith("ca-"))
                        .findAny()
                        .orElseThrow();
                X509Certificate oldX509ClusterCaCertStore = getCertificateFromTrustStore(oldClusterCaCertKey, clusterCaCertData);
                String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
                String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
                String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
                assertThat(newClusterCaCert, is(not(initialClusterCaCertSecret.getData().get(CA_CRT))));
                assertThat(newClusterCaCertStore, is(not(initialClusterCaCertSecret.getData().get(CA_STORE))));
                assertThat(newClusterCaCertStorePassword, is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(newX509ClusterCaCertStore, is(x509Certificate(newClusterCaCert)));
                Map.Entry<String, String> oldClusterCaCert = clusterCaCertData.entrySet().iterator().next();
                assertThat(oldClusterCaCert.getValue(), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                assertThat(oldX509ClusterCaCertStore, is(x509Certificate(String.valueOf(oldClusterCaCert.getValue()))));
                assertThat(x509Certificate(newClusterCaCert).getSubjectX500Principal().getName(), is("CN=cluster-ca v1,O=io.strimzi"));

                Secret clusterCaKeySecret = c.getAllValues().get(1);
                assertThat(clusterCaKeySecret.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1"));
                Map<String, String> clusterCaKeyData = clusterCaKeySecret.getData();
                assertThat(clusterCaKeyData.keySet(), is(singleton(CA_KEY)));
                String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
                assertThat(newClusterCaKey, is(notNullValue()));
                assertThat(newClusterCaKey, is(not(initialClusterCaKeySecret.getData().get(CA_KEY))));

                Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
                assertThat(clientsCaCertData, aMapWithSize(4));
                X509Certificate newX509ClientsCaCertStore = getCertificateFromTrustStore(CA_CRT, clientsCaCertData);
                String oldClientsCaCertKey = clientsCaCertData.keySet()
                        .stream()
                        .filter(alias -> alias.startsWith("ca-"))
                        .findAny()
                        .orElseThrow();
                X509Certificate oldX509ClientsCaCertStore = getCertificateFromTrustStore(oldClientsCaCertKey, clientsCaCertData);
                String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
                String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
                String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
                assertThat(newClientsCaCert, is(not(initialClientsCaCertSecret.getData().get(CA_CRT))));
                assertThat(newClientsCaCertStore, is(not(initialClientsCaCertSecret.getData().get(CA_STORE))));
                assertThat(newClientsCaCertStorePassword, is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(newX509ClientsCaCertStore, is(x509Certificate(newClientsCaCert)));
                Map.Entry<String, String> oldClientsCaCert = clientsCaCertData.entrySet().iterator().next();
                assertThat(oldClientsCaCert.getValue(), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                assertThat(oldX509ClientsCaCertStore, is(x509Certificate(String.valueOf(oldClientsCaCert.getValue()))));
                assertThat(x509Certificate(newClientsCaCert).getSubjectX500Principal().getName(), is("CN=clients-ca v1,O=io.strimzi"));

                Secret clientsCaKeySecret = c.getAllValues().get(3);
                assertThat(clientsCaKeySecret.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1"));
                Map<String, String> clientsCaKeyData = clientsCaKeySecret.getData();
                assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
                String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
                assertThat(newClientsCaKey, is(notNullValue()));
                assertThat(newClientsCaKey, is(not(initialClientsCaKeySecret.getData().get(CA_KEY))));
                async.flag();
            })));
    }

    @Test
    public void testNewKeyGeneratedWhenInRenewalPeriodAutoOutsideOfTimeWindow(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
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

        Checkpoint async = context.checkpoint();
        reconcileCa(vertx, kafka, Clock.fixed(Instant.parse("2018-11-26T09:00:00Z"), Clock.systemUTC().getZone()))
            .onComplete(context.succeeding(c -> context.verify(() -> {
                assertThat(c.getAllValues(), hasSize(4));

                Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
                assertThat(c.getAllValues().get(0).getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0"));
                assertThat(clusterCaCertData, aMapWithSize(3));
                X509Certificate newX509ClusterCaCertStore = getCertificateFromTrustStore(CA_CRT, clusterCaCertData);
                String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
                String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
                String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
                assertThat(newClusterCaCert, is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                assertThat(newClusterCaCertStore, is(initialClusterCaCertSecret.getData().get(CA_STORE)));
                assertThat(newClusterCaCertStorePassword, is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(newX509ClusterCaCertStore, is(x509Certificate(newClusterCaCert)));
                assertThat(x509Certificate(newClusterCaCert).getSubjectX500Principal().getName(), is("CN=cluster-ca,O=io.strimzi"));

                Secret clusterCaKeySecret = c.getAllValues().get(1);
                assertThat(clusterCaKeySecret.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0"));
                Map<String, String> clusterCaKeyData = clusterCaKeySecret.getData();
                assertThat(clusterCaKeyData.keySet(), is(singleton(CA_KEY)));
                String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
                assertThat(newClusterCaKey, is(notNullValue()));
                assertThat(newClusterCaKey, is(initialClusterCaKeySecret.getData().get(CA_KEY)));

                Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
                assertThat(c.getAllValues().get(2).getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0"));
                assertThat(clientsCaCertData, aMapWithSize(3));
                X509Certificate newX509ClientsCaCertStore = getCertificateFromTrustStore(CA_CRT, clientsCaCertData);
                String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
                String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
                String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
                assertThat(newClientsCaCert, is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                assertThat(newClientsCaCertStore, is(initialClientsCaCertSecret.getData().get(CA_STORE)));
                assertThat(newClientsCaCertStorePassword, is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(newX509ClientsCaCertStore, is(x509Certificate(newClientsCaCert)));
                assertThat(x509Certificate(newClientsCaCert).getSubjectX500Principal().getName(), is("CN=clients-ca,O=io.strimzi"));

                Secret clientsCaKeySecret = c.getAllValues().get(3);
                assertThat(clientsCaKeySecret.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "0"));
                Map<String, String> clientsCaKeyData = clientsCaKeySecret.getData();
                assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
                String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
                assertThat(newClientsCaKey, is(notNullValue()));
                assertThat(newClientsCaKey, is(initialClientsCaKeySecret.getData().get(CA_KEY)));

                async.flag();
            })));
    }

    @Test
    public void testNewKeyGeneratedWhenInRenewalPeriodAutoWithinTimeWindow(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
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

        Checkpoint async = context.checkpoint();
        reconcileCa(vertx, kafka, Clock.fixed(Instant.parse("2018-11-26T09:12:00Z"), Clock.systemUTC().getZone()))
            .onComplete(context.succeeding(c -> context.verify(() -> {

                assertThat(c.getAllValues(), hasSize(4));

                Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
                assertThat(c.getAllValues().get(0).getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"));
                assertThat(clusterCaCertData, aMapWithSize(4));
                X509Certificate newX509ClusterCaCertStore = getCertificateFromTrustStore(CA_CRT, clusterCaCertData);
                String oldClusterCaCertKey = clusterCaCertData.keySet()
                        .stream()
                        .filter(alias -> alias.startsWith("ca-"))
                        .findAny()
                        .orElseThrow();
                X509Certificate oldX509ClusterCaCertStore = getCertificateFromTrustStore(oldClusterCaCertKey, clusterCaCertData);
                String newClusterCaCert = clusterCaCertData.remove(CA_CRT);
                String newClusterCaCertStore = clusterCaCertData.remove(CA_STORE);
                String newClusterCaCertStorePassword = clusterCaCertData.remove(CA_STORE_PASSWORD);
                assertThat(newClusterCaCert, is(not(initialClusterCaCertSecret.getData().get(CA_CRT))));
                assertThat(newClusterCaCertStore, is(not(initialClusterCaCertSecret.getData().get(CA_STORE))));
                assertThat(newClusterCaCertStorePassword, is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(newX509ClusterCaCertStore, is(x509Certificate(newClusterCaCert)));
                Map.Entry<String, String> oldClusterCaCert = clusterCaCertData.entrySet().iterator().next();
                assertThat(oldClusterCaCert.getValue(), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                assertThat(oldX509ClusterCaCertStore, is(x509Certificate(String.valueOf(oldClusterCaCert.getValue()))));
                assertThat(x509Certificate(newClusterCaCert).getSubjectX500Principal().getName(), is("CN=cluster-ca v1,O=io.strimzi"));

                Secret clusterCaKeySecret = c.getAllValues().get(1);
                assertThat(clusterCaKeySecret.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1"));
                Map<String, String> clusterCaKeyData = clusterCaKeySecret.getData();
                assertThat(clusterCaKeyData.keySet(), is(singleton(CA_KEY)));
                String newClusterCaKey = clusterCaKeyData.remove(CA_KEY);
                assertThat(newClusterCaKey, is(notNullValue()));
                assertThat(newClusterCaKey, is(not(initialClusterCaKeySecret.getData().get(CA_KEY))));

                Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
                assertThat(c.getAllValues().get(2).getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"));
                assertThat(clientsCaCertData, aMapWithSize(4));
                X509Certificate newX509ClientsCaCertStore = getCertificateFromTrustStore(CA_CRT, clientsCaCertData);
                String oldClientsCaCertKey = clientsCaCertData.keySet()
                        .stream()
                        .filter(alias -> alias.startsWith("ca-"))
                        .findAny()
                        .orElseThrow();
                X509Certificate oldX509ClientsCaCertStore = getCertificateFromTrustStore(oldClientsCaCertKey, clientsCaCertData);
                String newClientsCaCert = clientsCaCertData.remove(CA_CRT);
                String newClientsCaCertStore = clientsCaCertData.remove(CA_STORE);
                String newClientsCaCertStorePassword = clientsCaCertData.remove(CA_STORE_PASSWORD);
                assertThat(newClientsCaCert, is(not(initialClientsCaCertSecret.getData().get(CA_CRT))));
                assertThat(newClientsCaCertStore, is(not(initialClientsCaCertSecret.getData().get(CA_STORE))));
                assertThat(newClientsCaCertStorePassword, is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(newX509ClientsCaCertStore, is(x509Certificate(newClientsCaCert)));
                Map.Entry<String, String> oldClientsCaCert = clientsCaCertData.entrySet().iterator().next();
                assertThat(oldClientsCaCert.getValue(), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                assertThat(oldX509ClientsCaCertStore, is(x509Certificate(String.valueOf(oldClientsCaCert.getValue()))));
                assertThat(x509Certificate(newClientsCaCert).getSubjectX500Principal().getName(), is("CN=clients-ca v1,O=io.strimzi"));

                Secret clientsCaKeySecret = c.getAllValues().get(3);
                assertThat(clientsCaKeySecret.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1"));
                Map<String, String> clientsCaKeyData = clientsCaKeySecret.getData();
                assertThat(clientsCaKeyData.keySet(), is(singleton(CA_KEY)));
                String newClientsCaKey = clientsCaKeyData.remove(CA_KEY);
                assertThat(newClientsCaKey, is(notNullValue()));
                assertThat(newClientsCaKey, is(not(initialClientsCaKeySecret.getData().get(CA_KEY))));

                async.flag();
            })));
    }

    private X509Certificate x509Certificate(String newClusterCaCert) throws CertificateException {
        return (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(Util.decodeBytesFromBase64(newClusterCaCert)));
    }


    @Test
    public void testExpiredCertsGetRemovedAuto(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
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
        String clusterCert = Objects.requireNonNull(TestUtils.readResource(getClass(), "cluster-ca.crt"));
        String encodedClusterCert = Base64.getEncoder().encodeToString(clusterCert.getBytes(StandardCharsets.UTF_8));
        initialClusterCaCertSecret.getData().put("ca-2018-07-01T09-00-00.crt", encodedClusterCert);

        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));
        // ... and to the related truststore
        Path certFile = Files.createTempFile("tls", "-cert");
        certFile.toFile().deleteOnExit();
        Path trustStoreFile = Files.createTempFile("tls", "-truststore");
        trustStoreFile.toFile().deleteOnExit();
        Files.write(certFile, Util.decodeBytesFromBase64(initialClusterCaCertSecret.getData().get("ca-2018-07-01T09-00-00.crt")));
        Files.write(trustStoreFile, Util.decodeBytesFromBase64(initialClusterCaCertSecret.getData().get(CA_STORE)));
        String trustStorePassword = Util.decodeFromBase64(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD));
        certManager.addCertToTrustStore(certFile.toFile(), "ca-2018-07-01T09-00-00.crt", trustStoreFile.toFile(), trustStorePassword);
        initialClusterCaCertSecret.getData().put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile)));
        assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", initialClusterCaCertSecret.getData()), is(true));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(set(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClientsCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()), is(true));

        // add an expired certificate to the secret ...
        String clientCert = Objects.requireNonNull(TestUtils.readResource(getClass(), "clients-ca.crt"));
        String encodedClientCert = Base64.getEncoder().encodeToString(clientCert.getBytes(StandardCharsets.UTF_8));
        initialClientsCaCertSecret.getData().put("ca-2018-07-01T09-00-00.crt", encodedClientCert);

        assertThat(initialClientsCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClientsCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        // ... and to the related truststore
        certFile = Files.createTempFile("tls", "-cert");
        certFile.toFile().deleteOnExit();
        Files.write(certFile, Util.decodeBytesFromBase64(initialClientsCaCertSecret.getData().get("ca-2018-07-01T09-00-00.crt")));
        trustStoreFile = Files.createTempFile("tls", "-truststore");
        trustStoreFile.toFile().deleteOnExit();
        Files.write(trustStoreFile, Util.decodeBytesFromBase64(initialClientsCaCertSecret.getData().get(CA_STORE)));
        trustStorePassword = Util.decodeFromBase64(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD));
        certManager.addCertToTrustStore(certFile.toFile(), "ca-2018-07-01T09-00-00.crt", trustStoreFile.toFile(), trustStorePassword);
        initialClientsCaCertSecret.getData().put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile)));
        assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", initialClientsCaCertSecret.getData()), is(true));

        secrets.add(initialClusterCaCertSecret);
        secrets.add(initialClusterCaKeySecret);
        secrets.add(initialClientsCaCertSecret);
        secrets.add(initialClientsCaKeySecret);

        Checkpoint async = context.checkpoint();
        reconcileCa(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.succeeding(c -> context.verify(() -> {
                assertThat(c.getAllValues(), hasSize(4));

                Map<String, String> clusterCaCertData = c.getAllValues().get(0).getData();
                assertThat(clusterCaCertData, aMapWithSize(3));
                assertThat(clusterCaCertData.get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                assertThat(clusterCaCertData.get(CA_STORE), is(initialClusterCaCertSecret.getData().get(CA_STORE)));
                assertThat(clusterCaCertData.get(CA_STORE_PASSWORD), is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(getCertificateFromTrustStore(CA_CRT, clusterCaCertData), is(x509Certificate(clusterCaCertData.get(CA_CRT))));
                Map<String, String> clusterCaKeyData = c.getAllValues().get(1).getData();
                assertThat(clusterCaKeyData.get(CA_KEY), is(initialClusterCaKeySecret.getData().get(CA_KEY)));
                assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", clusterCaCertData), is(false));

                Map<String, String> clientsCaCertData = c.getAllValues().get(2).getData();
                assertThat(clientsCaCertData, aMapWithSize(3));
                assertThat(clientsCaCertData.get(CA_CRT), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                assertThat(clientsCaCertData.get(CA_STORE), is(initialClientsCaCertSecret.getData().get(CA_STORE)));
                assertThat(clientsCaCertData.get(CA_STORE_PASSWORD), is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                assertThat(getCertificateFromTrustStore(CA_CRT, clientsCaCertData), is(x509Certificate(clientsCaCertData.get(CA_CRT))));
                Map<String, String> clientsCaKeyData = c.getAllValues().get(3).getData();
                assertThat(clientsCaKeyData.get(CA_KEY), is(initialClientsCaKeySecret.getData().get(CA_KEY)));
                assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", clientsCaCertData), is(false));
                async.flag();
            })));
    }

    @Test
    public void testCustomCertsNotReconciled(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
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

        Checkpoint async = context.checkpoint();
        reconcileCa(vertx, certificateAuthority, certificateAuthority)
            .onComplete(context.succeeding(c -> context.verify(() -> {
                assertThat(c.getAllValues(), hasSize(0));
                async.flag();
            })));
    }

    @Test
    public void testCustomLabelsAndAnnotations(Vertx vertx, VertxTestContext context) {
        Map<String, String> labels = new HashMap<>(2);
        labels.put("label1", "value1");
        labels.put("label2", "value2");

        Map<String, String> annos = new HashMap<>(2);
        annos.put("anno1", "value3");
        annos.put("anno2", "value4");

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                        .withNewTemplate()
                            .withNewClusterCaCert()
                                .withNewMetadata()
                                    .withAnnotations(annos)
                                    .withLabels(labels)
                                .endMetadata()
                            .endClusterCaCert()
                        .endTemplate()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        SecretOperator secretOps = supplier.secretOperations;
        PodOperator podOps = supplier.podOperations;

        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), clientsCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.secretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        when(podOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        new CaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, certManager, passwordGenerator)
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(clusterCaCert.getAllValues(), hasSize(1));
                    assertThat(clusterCaKey.getAllValues(), hasSize(1));
                    assertThat(clientsCaCert.getAllValues(), hasSize(1));
                    assertThat(clientsCaKey.getAllValues(), hasSize(1));

                    Secret clusterCaCertSecret = clusterCaCert.getValue();
                    Secret clusterCaKeySecret = clusterCaKey.getValue();
                    Secret clientsCaCertSecret = clientsCaCert.getValue();
                    Secret clientsCaKeySecret = clientsCaKey.getValue();

                    for (Map.Entry<String, String> entry : annos.entrySet()) {
                        assertThat(clusterCaCertSecret.getMetadata().getAnnotations(), hasEntry(entry.getKey(), entry.getValue()));
                        assertThat(clusterCaKeySecret.getMetadata().getAnnotations(), not(hasEntry(entry.getKey(), entry.getValue())));
                        assertThat(clientsCaCertSecret.getMetadata().getAnnotations(), not(hasEntry(entry.getKey(), entry.getValue())));
                        assertThat(clientsCaKeySecret.getMetadata().getAnnotations(), not(hasEntry(entry.getKey(), entry.getValue())));
                    }

                    for (Map.Entry<String, String> entry : labels.entrySet()) {
                        assertThat(clusterCaCertSecret.getMetadata().getLabels(), hasEntry(entry.getKey(), entry.getValue()));
                        assertThat(clusterCaKeySecret.getMetadata().getLabels(), not(hasEntry(entry.getKey(), entry.getValue())));
                        assertThat(clientsCaCertSecret.getMetadata().getLabels(), not(hasEntry(entry.getKey(), entry.getValue())));
                        assertThat(clientsCaKeySecret.getMetadata().getLabels(), not(hasEntry(entry.getKey(), entry.getValue())));
                    }

                    async.flag();
                })));
    }

    @Test
    public void testClusterCASecretsWithoutOwnerReference(Vertx vertx, VertxTestContext context) {
        CertificateAuthority caConfig = new CertificateAuthority();
        caConfig.setGenerateSecretOwnerReference(false);

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                        .withName(NAME)
                        .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withClusterCa(caConfig)
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

        OwnerReference ownerReference = new OwnerReferenceBuilder()
                .withKind(kafka.getKind())
                .withApiVersion(kafka.getApiVersion())
                .withName(kafka.getMetadata().getName())
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        SecretOperator secretOps = supplier.secretOperations;
        PodOperator podOps = supplier.podOperations;

        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), clientsCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.secretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        when(podOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        new CaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, certManager, passwordGenerator)
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(clusterCaCert.getAllValues(), hasSize(1));
                    assertThat(clusterCaKey.getAllValues(), hasSize(1));
                    assertThat(clientsCaCert.getAllValues(), hasSize(1));
                    assertThat(clientsCaKey.getAllValues(), hasSize(1));

                    Secret clusterCaCertSecret = clusterCaCert.getValue();
                    Secret clusterCaKeySecret = clusterCaKey.getValue();
                    Secret clientsCaCertSecret = clientsCaCert.getValue();
                    Secret clientsCaKeySecret = clientsCaKey.getValue();

                    assertThat(clusterCaCertSecret.getMetadata().getOwnerReferences(), hasSize(0));
                    assertThat(clusterCaKeySecret.getMetadata().getOwnerReferences(), hasSize(0));
                    assertThat(clientsCaCertSecret.getMetadata().getOwnerReferences(), hasSize(1));
                    assertThat(clientsCaKeySecret.getMetadata().getOwnerReferences(), hasSize(1));

                    assertThat(clientsCaCertSecret.getMetadata().getOwnerReferences().get(0), is(ownerReference));
                    assertThat(clientsCaKeySecret.getMetadata().getOwnerReferences().get(0), is(ownerReference));

                    async.flag();
                })));
    }

    @Test
    public void testClientsCASecretsWithoutOwnerReference(Vertx vertx, VertxTestContext context) {
        CertificateAuthority caConfig = new CertificateAuthority();
        caConfig.setGenerateSecretOwnerReference(false);

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                        .withName(NAME)
                        .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withClientsCa(caConfig)
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

        OwnerReference ownerReference = new OwnerReferenceBuilder()
                .withKind(kafka.getKind())
                .withApiVersion(kafka.getApiVersion())
                .withName(kafka.getMetadata().getName())
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        SecretOperator secretOps = supplier.secretOperations;
        PodOperator podOps = supplier.podOperations;

        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), clientsCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.secretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        when(podOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        new CaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, certManager, passwordGenerator)
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(clusterCaCert.getAllValues(), hasSize(1));
                    assertThat(clusterCaKey.getAllValues(), hasSize(1));
                    assertThat(clientsCaCert.getAllValues(), hasSize(1));
                    assertThat(clientsCaKey.getAllValues(), hasSize(1));

                    Secret clusterCaCertSecret = clusterCaCert.getValue();
                    Secret clusterCaKeySecret = clusterCaKey.getValue();
                    Secret clientsCaCertSecret = clientsCaCert.getValue();
                    Secret clientsCaKeySecret = clientsCaKey.getValue();

                    assertThat(clusterCaCertSecret.getMetadata().getOwnerReferences(), hasSize(1));
                    assertThat(clusterCaKeySecret.getMetadata().getOwnerReferences(), hasSize(1));
                    assertThat(clientsCaCertSecret.getMetadata().getOwnerReferences(), hasSize(0));
                    assertThat(clientsCaKeySecret.getMetadata().getOwnerReferences(), hasSize(0));

                    assertThat(clusterCaCertSecret.getMetadata().getOwnerReferences().get(0), is(ownerReference));
                    assertThat(clusterCaKeySecret.getMetadata().getOwnerReferences().get(0), is(ownerReference));

                    async.flag();
                })));
    }

    @Test
    public void testClusterCAKeyNotTrusted(Vertx vertx, VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture())).thenAnswer(i -> {
            Secret s = clusterCaCert.getValue();
            s.getMetadata().setAnnotations(Map.of(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"));
            return Future.succeededFuture(ReconcileResult.created(s));
        });
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture())).thenAnswer(i -> {
            Secret s = clusterCaKey.getValue();
            s.getMetadata().setAnnotations(Map.of(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1"));
            return Future.succeededFuture(ReconcileResult.created(s));
        });
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), clientsCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.secretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0", Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0");

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenAnswer(i -> {
            List<Pod> pods = new ArrayList<>();
            // adding a terminating Cruise Control pod to test that it's skipped during the key generation check
            Pod ccPod = podWithNameAndAnnotations("my-kafka-cruise-control", generationAnnotations);
            ccPod.getMetadata().setDeletionTimestamp("2023-06-08T16:23:18Z");
            pods.add(ccPod);
            // adding ZooKeeper and Kafka pods with old CA cert and key generation
            pods.add(podWithNameAndAnnotations("my-kafka-zookeeper-0", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-zookeeper-1", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-zookeeper-2", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-kafka-0", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-kafka-1", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-kafka-2", generationAnnotations));
            return Future.succeededFuture(pods);
        });

        Checkpoint async = context.checkpoint();

        CaReconciler caReconciler = new CaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, certManager, passwordGenerator);
        caReconciler
                .reconcileCas(Clock.systemUTC())
                .compose(i -> caReconciler.verifyClusterCaFullyTrustedAndUsed())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(caReconciler.isClusterCaNeedFullTrust, is(true));
                    async.flag();
                })));
    }

    @Test
    public void testRollingReasonsWithClusterCAKeyNotTrusted(Vertx vertx, VertxTestContext context) {

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                    .withNewEntityOperator()
                    .endEntityOperator()
                    .withNewCruiseControl()
                    .endCruiseControl()
                    .withNewKafkaExporter()
                    .endKafkaExporter()
                .endSpec()
                .build();

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clusterCaKey = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);
        ArgumentCaptor<Secret> clientsCaKey = ArgumentCaptor.forClass(Secret.class);
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture())).thenAnswer(i -> {
            Secret s = clusterCaCert.getValue();
            s.getMetadata().setAnnotations(Map.of(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1"));
            return Future.succeededFuture(ReconcileResult.created(s));
        });
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaKeySecretName(NAME)), clusterCaKey.capture())).thenAnswer(i -> {
            Secret s = clusterCaKey.getValue();
            s.getMetadata().setAnnotations(Map.of(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1"));
            return Future.succeededFuture(ReconcileResult.created(s));
        });
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaKeySecretName(NAME)), clientsCaKey.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.secretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0", Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0");

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenAnswer(i -> {
            List<Pod> pods = new ArrayList<>();
            // adding a terminating Cruise Control pod to test that it's skipped during the key generation check
            Pod ccPod = podWithNameAndAnnotations("my-kafka-cruise-control", generationAnnotations);
            ccPod.getMetadata().setDeletionTimestamp("2023-06-08T16:23:18Z");
            pods.add(ccPod);
            // adding ZooKeeper and Kafka pods with old CA cert and key generation
            pods.add(podWithNameAndAnnotations("my-kafka-zookeeper-0", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-zookeeper-1", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-zookeeper-2", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-kafka-0", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-kafka-1", generationAnnotations));
            pods.add(podWithNameAndAnnotations("my-kafka-kafka-2", generationAnnotations));
            return Future.succeededFuture(pods);
        });

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-kafka-entity-operator", deploymentWithName("my-kafka-entity-operator"));
        deps.put("my-kafka-cruise-control", deploymentWithName("my-kafka-cruise-control"));
        deps.put("my-kafka-kafka-exporter", deploymentWithName("my-kafka-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> Future.succeededFuture(deps.get(i.getArgument(1))));

        Checkpoint async = context.checkpoint();

        MockCaReconciler mockCaReconciler = new MockCaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, certManager, passwordGenerator);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(mockCaReconciler.isClusterCaNeedFullTrust, is(true));
                    assertThat(mockCaReconciler.zkPodRestartReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED), is(true));
                    assertThat(mockCaReconciler.kPodRollReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED), is(true));
                    assertThat(mockCaReconciler.deploymentRollReason.size() == 3, is(true));
                    for (String reason: mockCaReconciler.deploymentRollReason) {
                        assertThat(reason.equals(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED.getDefaultNote()), is(true));
                    }
                    async.flag();
                })));
    }

    static class MockCaReconciler extends CaReconciler {

        RestartReasons zkPodRestartReasons;
        RestartReasons kPodRollReasons;
        List<String> deploymentRollReason = new ArrayList<>();

        public MockCaReconciler(Reconciliation reconciliation, Kafka kafkaCr, ClusterOperatorConfig config, ResourceOperatorSupplier supplier, Vertx vertx, CertManager certManager, PasswordGenerator passwordGenerator) {
            super(reconciliation, kafkaCr, config, supplier, vertx, certManager, passwordGenerator);
        }

        @Override
        Future<Void> verifyClusterCaFullyTrustedAndUsed() {
            // assuming the CA key is not trusted
            this.isClusterCaNeedFullTrust = true;
            this.isClusterCaFullyUsed = false;
            return Future.succeededFuture();
        }

        @Override
        Future<Integer> getZooKeeperReplicas() {
            return Future.succeededFuture(3);
        }

        @Override
        Future<Void> maybeRollZookeeper(int replicas, RestartReasons podRestartReasons, TlsPemIdentity coTlsPemIdentity) {
            this.zkPodRestartReasons = podRestartReasons;
            return Future.succeededFuture();
        }

        @Override
        Future<Set<NodeRef>> getKafkaReplicas() {
            Set<NodeRef> nodes = new HashSet<>();
            nodes.add(ReconcilerUtils.nodeFromPod(podWithName("my-kafka-kafka-0")));
            nodes.add(ReconcilerUtils.nodeFromPod(podWithName("my-kafka-kafka-1")));
            nodes.add(ReconcilerUtils.nodeFromPod(podWithName("my-kafka-kafka-2")));
            return Future.succeededFuture(nodes);
        }

        @Override
        Future<Void> rollKafkaBrokers(Set<NodeRef> nodes, RestartReasons podRollReasons, TlsPemIdentity coTlsPemIdentity) {
            this.kPodRollReasons = podRollReasons;
            return Future.succeededFuture();
        }

        @Override
        Future<Void> rollDeploymentIfExists(String deploymentName, String reason) {
            return deploymentOperator.getAsync(reconciliation.namespace(), deploymentName)
                    .compose(dep -> {
                        if (dep != null) {
                            this.deploymentRollReason.add(reason);
                        }
                        return Future.succeededFuture();
                    });
        }

        @Override
        Future<Void> maybeRemoveOldClusterCaCertificates() {
            return Future.succeededFuture();
        }
    }

    public static Pod podWithName(String name) {
        return podWithNameAndAnnotations(name, Collections.emptyMap());
    }

    public static Pod podWithNameAndAnnotations(String name, Map<String, String> annotations) {
        return new PodBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withAnnotations(annotations)
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, NAME))
                .endMetadata()
                .build();
    }

    public static Deployment deploymentWithName(String name) {
        return new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .build();
    }
}
