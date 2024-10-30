/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.api.ResourceAnnotations;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.common.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
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
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.test.ReadWriteUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.model.Ca.CA_CRT;
import static io.strimzi.operator.common.model.Ca.CA_KEY;
import static io.strimzi.operator.common.model.Ca.CA_STORE;
import static io.strimzi.operator.common.model.Ca.CA_STORE_PASSWORD;
import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
@ExtendWith(VertxExtension.class)
public class CaReconcilerTest {
    private static final String NAMESPACE = "test";
    private static final String NAME = "my-cluster";
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled", Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"))
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build())
                .endKafka()
            .endSpec()
            .build();

    private final static OpenSslCertManager CERT_MANAGER = new OpenSslCertManager();
    private final static PasswordGenerator PASSWORD_GENERATOR = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");

    private final List<Secret> secrets = new ArrayList<>();
    private WorkerExecutor sharedWorkerExecutor;

    @BeforeEach
    public void setup(Vertx vertx) {
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterEach
    public void teardown() {
        sharedWorkerExecutor.close();
    }

    private Future<ArgumentCaptor<Secret>> reconcileCa(Vertx vertx, CertificateAuthority clusterCa, CertificateAuthority clientsCa) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
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
        when(secretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)))).thenAnswer(i -> Future.succeededFuture());
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));

        when(deploymentOps.getAsync(eq(NAMESPACE), any())).thenReturn(Future.succeededFuture());

        when(spsOps.getAsync(eq(NAMESPACE), any())).thenReturn(Future.succeededFuture());
        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture());

        when(podOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Promise<ArgumentCaptor<Secret>> reconcileCasComplete = Promise.promise();

        new CaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR)
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

    private CertAndKey generateCa(CertificateAuthority certificateAuthority, String commonName)
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

        CERT_MANAGER.generateSelfSignedCert(clusterCaKeyFile.toFile(), clusterCaCertFile.toFile(), sbj, ModelUtils.getCertificateValidity(certificateAuthority));

        CERT_MANAGER.addCertToTrustStore(clusterCaCertFile.toFile(), CA_CRT, clusterCaStoreFile.toFile(), clusterCaStorePassword);
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
        CertAndKey result = generateCa(certificateAuthority, commonName);
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

                assertThat(c.getAllValues().get(0).getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
                assertThat(isCertInTrustStore(CA_CRT, c.getAllValues().get(0).getData()), is(true));

                assertThat(c.getAllValues().get(1).getData().keySet(), is(singleton(CA_KEY)));

                assertThat(c.getAllValues().get(2).getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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
        assertThat(clusterCaCertData.keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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
        assertThat(clientsCaCertData.keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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

                assertThat(c.getAllValues().get(0).getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
                assertThat(c.getAllValues().get(0).getData().get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                assertThat(x509Certificate(initialClusterCaCertSecret.getData().get(CA_CRT)), is(getCertificateFromTrustStore(CA_CRT, c.getAllValues().get(0).getData())));

                assertThat(c.getAllValues().get(1).getData().keySet(), is(Set.of(CA_KEY)));
                assertThat(c.getAllValues().get(1).getData().get(CA_KEY), is(initialClusterCaKeySecret.getData().get(CA_KEY)));

                assertThat(c.getAllValues().get(2).getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
                assertThat(c.getAllValues().get(2).getData().get(CA_CRT), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                assertThat(x509Certificate(initialClientsCaCertSecret.getData().get(CA_CRT)), is(getCertificateFromTrustStore(CA_CRT, c.getAllValues().get(2).getData())));

                assertThat(c.getAllValues().get(3).getData().keySet(), is(Set.of(CA_KEY)));
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
                assertThat(clusterCaCertData.keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));

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
                assertThat(clientsCaCertData.keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));

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
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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
                assertThat(clusterCaCertData.keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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
                assertThat(clientsCaCertData.keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));

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

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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
                assertThat(clusterCaCertData.keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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
                assertThat(clientsCaCertData.keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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
                assertThat(clusterCaCertData.keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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
                assertThat(clientsCaCertData.keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClusterCa(certificateAuthority)
                    .withClientsCa(certificateAuthority)
                    .withMaintenanceTimeWindows("* 10-14 * * * ? *")
                .endSpec()
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));

        // add an expired certificate to the secret ...
        String clusterCert = Objects.requireNonNull(ReadWriteUtils.readFileFromResources(getClass(), "cluster-ca.crt"));
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
        CERT_MANAGER.addCertToTrustStore(certFile.toFile(), "ca-2018-07-01T09-00-00.crt", trustStoreFile.toFile(), trustStorePassword);
        initialClusterCaCertSecret.getData().put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile)));
        assertThat(isCertInTrustStore("ca-2018-07-01T09-00-00.crt", initialClusterCaCertSecret.getData()), is(true));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClientsCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClientsCaCertSecret.getData()), is(true));

        // add an expired certificate to the secret ...
        String clientCert = Objects.requireNonNull(ReadWriteUtils.readFileFromResources(getClass(), "clients-ca.crt"));
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
        CERT_MANAGER.addCertToTrustStore(certFile.toFile(), "ca-2018-07-01T09-00-00.crt", trustStoreFile.toFile(), trustStorePassword);
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
        assertThat(initialClusterCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
        assertThat(initialClusterCaCertSecret.getData().get(CA_CRT), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE), is(notNullValue()));
        assertThat(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD), is(notNullValue()));
        assertThat(isCertInTrustStore(CA_CRT, initialClusterCaCertSecret.getData()), is(true));
        assertThat(initialClusterCaKeySecret.getData().keySet(), is(singleton(CA_KEY)));
        assertThat(initialClusterCaKeySecret.getData().get(CA_KEY), is(notNullValue()));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);
        assertThat(initialClientsCaCertSecret.getData().keySet(), is(Set.of(CA_CRT, CA_STORE, CA_STORE_PASSWORD)));
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

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewClusterCaCert()
                                .withNewMetadata()
                                    .withAnnotations(annos)
                                    .withLabels(labels)
                                .endMetadata()
                            .endClusterCaCert()
                        .endTemplate()
                    .endKafka()
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
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        when(podOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        new CaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR)
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

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClusterCa(caConfig)
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
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        when(podOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        new CaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR)
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

                    TestUtils.checkOwnerReference(clientsCaCertSecret, kafka);
                    TestUtils.checkOwnerReference(clientsCaKeySecret, kafka);

                    async.flag();
                })));
    }

    @Test
    public void testClientsCASecretsWithoutOwnerReference(Vertx vertx, VertxTestContext context) {
        CertificateAuthority caConfig = new CertificateAuthority();
        caConfig.setGenerateSecretOwnerReference(false);

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withClientsCa(caConfig)
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
        when(secretOps.reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(0))));
        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        when(podOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        new CaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR)
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

                    TestUtils.checkOwnerReference(clusterCaCertSecret, kafka);
                    TestUtils.checkOwnerReference(clusterCaKeySecret, kafka);

                    async.flag();
                })));
    }

    //////////
    // Tests for trust rollout
    //////////

    @Test
    public void testStrimziManagedClusterCaKeyReplaced(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        //Annotate Cluster CA key to force replacement
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0).edit()
                .editMetadata()
                    .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_REPLACE, "true")
                .endMetadata()
                .build();
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(
                initialClusterCaKeySecret, initialClusterCaCertSecret,
                initialClientsCaKeySecret, initialClientsCaCertSecret)));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        // Kafka pods with old CA cert and key generation
        List<Pod> controllerPods = new ArrayList<>();
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-3", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-4", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-5", false, true, generationAnnotations));
        List<Pod> brokerPods = new ArrayList<>();
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-1", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-2", true, false, generationAnnotations));
        List<Pod> pods = new ArrayList<>(controllerPods);
        pods.addAll(brokerPods);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
        when(spsOps.getAsync(eq(NAMESPACE), eq(KafkaResources.zookeeperComponentName(NAME)))).thenReturn(Future.succeededFuture());
        StrimziPodSet controllerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-controller")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(controllerPods))
                .endSpec()
                .build();

        StrimziPodSet brokerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-broker")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(brokerPods))
                .endSpec()
                .build();

        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(controllerPodSet, brokerPodSet)));

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-cluster-entity-operator", deploymentWithName("my-cluster-entity-operator"));
        deps.put("my-cluster-cruise-control", deploymentWithName("my-cluster-cruise-control"));
        deps.put("my-cluster-kafka-exporter", deploymentWithName("my-cluster-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> Future.succeededFuture(deps.get(i.getArgument(1, String.class))));

        Checkpoint async = context.checkpoint();

        NewMockCaReconciler mockCaReconciler = new NewMockCaReconciler(reconciliation, KAFKA, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, aMapWithSize(6));
                    mockCaReconciler.kafkaRestartReasons.forEach((podName, restartReasons) -> {
                        assertThat("Restart reasons for pod " + podName, restartReasons.getReasons(), hasSize(1));
                        assertThat("Restart reasons for pod " + podName, restartReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED), is(true));
                    });
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, aMapWithSize(3));
                    mockCaReconciler.deploymentRestartReasons.forEach((deploymentName, restartReason) ->
                            assertThat("Deployment restart reason for " + deploymentName, restartReason.equals(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED.getDefaultNote()), is(true)));
                    async.flag();
                })));
    }

    // Strimzi Cluster CA key replaced in previous reconcile and some pods already rolled
    @Test
    public void testStrimziManagedClusterCaKeyReplacedPreviously(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        // Edit Cluster CA key and cert to increment generation as though replacement happened in previous reconcile
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1")
                .endMetadata()
                .build();
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1")
                .endMetadata()
                .build();

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(
                initialClusterCaKeySecret, initialClusterCaCertSecret,
                initialClientsCaKeySecret, initialClientsCaCertSecret)));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        // Kafka pods with old CA cert and key generation
        List<Pod> controllerPods = new ArrayList<>();
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-3", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-4", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-5", false, true, generationAnnotations));
        List<Pod> brokerPods = new ArrayList<>();
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-1", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-2", true, false, generationAnnotations));
        List<Pod> pods = new ArrayList<>(controllerPods);
        pods.addAll(brokerPods);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
        when(spsOps.getAsync(eq(NAMESPACE), eq(KafkaResources.zookeeperComponentName(NAME)))).thenReturn(Future.succeededFuture());
        StrimziPodSet controllerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-controller")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(controllerPods))
                .endSpec()
                .build();

        StrimziPodSet brokerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-broker")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(brokerPods))
                .endSpec()
                .build();

        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(controllerPodSet, brokerPodSet)));

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-cluster-entity-operator", deploymentWithName("my-cluster-entity-operator"));
        deps.put("my-cluster-cruise-control", deploymentWithName("my-cluster-cruise-control"));
        deps.put("my-cluster-kafka-exporter", deploymentWithName("my-cluster-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> Future.succeededFuture(deps.get(i.getArgument(1, String.class))));

        Checkpoint async = context.checkpoint();

        NewMockCaReconciler mockCaReconciler = new NewMockCaReconciler(reconciliation, KAFKA, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, aMapWithSize(6));
                    mockCaReconciler.kafkaRestartReasons.forEach((podName, restartReasons) -> {
                        assertThat("Restart reasons for pod " + podName, restartReasons.getReasons(), hasSize(1));
                        assertThat("Restart reasons for pod " + podName, restartReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED), is(true));
                    });
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, aMapWithSize(3));
                    mockCaReconciler.deploymentRestartReasons.forEach((deploymentName, restartReason) ->
                            assertThat("Deployment restart reason for " + deploymentName, restartReason.equals(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED.getDefaultNote()), is(true)));
                    async.flag();
                })));
    }

    @Test
    public void testStrimziManagedClusterCaCertRenewed(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        //Annotate Cluster CA cert to force renewal
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true")
                .endMetadata()
                .build();

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(
                initialClusterCaKeySecret, initialClusterCaCertSecret,
                initialClientsCaKeySecret, initialClientsCaCertSecret)));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        // Kafka pods with old CA cert and key generation
        List<Pod> controllerPods = new ArrayList<>();
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-3", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-4", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-5", false, true, generationAnnotations));
        List<Pod> brokerPods = new ArrayList<>();
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-1", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-2", true, false, generationAnnotations));
        List<Pod> pods = new ArrayList<>(controllerPods);
        pods.addAll(brokerPods);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
        when(spsOps.getAsync(eq(NAMESPACE), eq(KafkaResources.zookeeperComponentName(NAME)))).thenReturn(Future.succeededFuture());
        StrimziPodSet controllerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-controller")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(controllerPods))
                .endSpec()
                .build();

        StrimziPodSet brokerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-broker")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(brokerPods))
                .endSpec()
                .build();

        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(controllerPodSet, brokerPodSet)));

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-cluster-entity-operator", deploymentWithName("my-cluster-entity-operator"));
        deps.put("my-cluster-cruise-control", deploymentWithName("my-cluster-cruise-control"));
        deps.put("my-cluster-kafka-exporter", deploymentWithName("my-cluster-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> Future.succeededFuture(deps.get(i.getArgument(1, String.class))));

        Checkpoint async = context.checkpoint();

        NewMockCaReconciler mockCaReconciler = new NewMockCaReconciler(reconciliation, KAFKA, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());
                    async.flag();
                })));
    }

    @Test
    public void testUserManagedClusterCaKeyReplaced(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        // Edit Cluster CA key and cert to increment generation as though user has replaced CA key
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1")
                .endMetadata()
                .build();
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1")
                .endMetadata()
                .build();

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        // Kafka brokers Secret with old annotation
        Secret kafkaBrokersSecret = kafkaBrokersSecretWithAnnotations(Map.of(
                Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(
                initialClusterCaKeySecret, initialClusterCaCertSecret,
                initialClientsCaKeySecret, initialClientsCaCertSecret,
                kafkaBrokersSecret)));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        // Kafka pods with old CA cert and key generation
        List<Pod> controllerPods = new ArrayList<>();
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-3", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-4", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-5", false, true, generationAnnotations));
        List<Pod> brokerPods = new ArrayList<>();
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-1", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-2", true, false, generationAnnotations));
        List<Pod> pods = new ArrayList<>(controllerPods);
        pods.addAll(brokerPods);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
        when(spsOps.getAsync(eq(NAMESPACE), eq(KafkaResources.zookeeperComponentName(NAME)))).thenReturn(Future.succeededFuture());
        StrimziPodSet controllerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-controller")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(controllerPods))
                .endSpec()
                .build();

        StrimziPodSet brokerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-broker")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(brokerPods))
                .endSpec()
                .build();

        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(controllerPodSet, brokerPodSet)));

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-cluster-entity-operator", deploymentWithName("my-cluster-entity-operator"));
        deps.put("my-cluster-cruise-control", deploymentWithName("my-cluster-cruise-control"));
        deps.put("my-cluster-kafka-exporter", deploymentWithName("my-cluster-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> Future.succeededFuture(deps.get(i.getArgument(1, String.class))));

        Checkpoint async = context.checkpoint();

        // Disable CA generation
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editClusterCa()
                        .withGenerateCertificateAuthority(false)
                    .endClusterCa()
                .endSpec()
                .build();
        NewMockCaReconciler mockCaReconciler = new NewMockCaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, aMapWithSize(6));
                    mockCaReconciler.kafkaRestartReasons.forEach((podName, restartReasons) -> {
                        assertThat("Restart reasons for pod " + podName, restartReasons.getReasons(), hasSize(1));
                        assertThat("Restart reasons for pod " + podName, restartReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED), is(true));
                    });
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, aMapWithSize(3));
                    mockCaReconciler.deploymentRestartReasons.forEach((deploymentName, restartReason) ->
                            assertThat("Deployment restart reason for " + deploymentName, restartReason.equals(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED.getDefaultNote()), is(true)));
                    async.flag();
                })));
    }

    @Test
    public void testUserManagedClusterCaCertRenewed(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        // Edit Cluster CA cert to increment generation as though user has renewed CA cert
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1")
                .endMetadata()
                .build();

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        // Kafka brokers Secret with old annotation
        Secret kafkaBrokersSecret = kafkaBrokersSecretWithAnnotations(Map.of(
                Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(
                initialClusterCaKeySecret, initialClusterCaCertSecret,
                initialClientsCaKeySecret, initialClientsCaCertSecret,
                kafkaBrokersSecret)));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        // Kafka pods with old CA cert and key generation
        List<Pod> controllerPods = new ArrayList<>();
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-3", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-4", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-5", false, true, generationAnnotations));
        List<Pod> brokerPods = new ArrayList<>();
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-1", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-2", true, false, generationAnnotations));
        List<Pod> pods = new ArrayList<>(controllerPods);
        pods.addAll(brokerPods);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
        when(spsOps.getAsync(eq(NAMESPACE), eq(KafkaResources.zookeeperComponentName(NAME)))).thenReturn(Future.succeededFuture());
        StrimziPodSet controllerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-controller")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(controllerPods))
                .endSpec()
                .build();

        StrimziPodSet brokerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-broker")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(brokerPods))
                .endSpec()
                .build();

        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(controllerPodSet, brokerPodSet)));

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-cluster-entity-operator", deploymentWithName("my-cluster-entity-operator"));
        deps.put("my-cluster-cruise-control", deploymentWithName("my-cluster-cruise-control"));
        deps.put("my-cluster-kafka-exporter", deploymentWithName("my-cluster-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> Future.succeededFuture(deps.get(i.getArgument(1, String.class))));

        Checkpoint async = context.checkpoint();

        // Disable CA generation
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editClusterCa()
                        .withGenerateCertificateAuthority(false)
                    .endClusterCa()
                .endSpec()
                .build();
        NewMockCaReconciler mockCaReconciler = new NewMockCaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    // When user is managing CA a cert renewal implies a key replacement
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, aMapWithSize(6));
                    mockCaReconciler.kafkaRestartReasons.forEach((podName, restartReasons) -> {
                        assertThat("Restart reasons for pod " + podName, restartReasons.getReasons(), hasSize(1));
                        assertThat("Restart reasons for pod " + podName, restartReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED), is(true));
                    });
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, aMapWithSize(3));
                    mockCaReconciler.deploymentRestartReasons.forEach((deploymentName, restartReason) ->
                            assertThat("Deployment restart reason for " + deploymentName, restartReason.equals(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED.getDefaultNote()), is(true)));
                    async.flag();
                })));
    }

    @Test
    public void testStrimziManagedClientsCaKeyReplaced(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        //Annotate Clients CA key to force replacement
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0).edit()
                .editMetadata()
                    .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_REPLACE, "true")
                .endMetadata()
                .build();
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(
                initialClusterCaKeySecret, initialClusterCaCertSecret,
                initialClientsCaKeySecret, initialClientsCaCertSecret)));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        // Kafka pods with old CA cert and key generation
        List<Pod> controllerPods = new ArrayList<>();
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-3", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-4", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-5", false, true, generationAnnotations));
        List<Pod> brokerPods = new ArrayList<>();
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-1", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-2", true, false, generationAnnotations));
        List<Pod> pods = new ArrayList<>(controllerPods);
        pods.addAll(brokerPods);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
        when(spsOps.getAsync(eq(NAMESPACE), eq(KafkaResources.zookeeperComponentName(NAME)))).thenReturn(Future.succeededFuture());
        StrimziPodSet controllerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-controller")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(controllerPods))
                .endSpec()
                .build();

        StrimziPodSet brokerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-broker")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(brokerPods))
                .endSpec()
                .build();

        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(controllerPodSet, brokerPodSet)));

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-cluster-entity-operator", deploymentWithName("my-cluster-entity-operator"));
        deps.put("my-cluster-cruise-control", deploymentWithName("my-cluster-cruise-control"));
        deps.put("my-cluster-kafka-exporter", deploymentWithName("my-cluster-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> Future.succeededFuture(deps.get(i.getArgument(1, String.class))));

        Checkpoint async = context.checkpoint();

        NewMockCaReconciler mockCaReconciler = new NewMockCaReconciler(reconciliation, KAFKA, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, aMapWithSize(6));
                    mockCaReconciler.kafkaRestartReasons.forEach((podName, restartReasons) -> {
                        assertThat("Restart reasons for pod " + podName, restartReasons.getReasons(), hasSize(1));
                        assertThat("Restart reasons for pod " + podName, restartReasons.contains(RestartReason.CLIENT_CA_CERT_KEY_REPLACED), is(true));
                    });
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());
                    async.flag();
                })));
    }

    // Strimzi Clients CA key replaced in previous reconcile and some pods already rolled
    @Test
    public void testStrimziManagedClientsCaKeyReplacedPreviously(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        // Edit Clients CA key and cert to increment generation as though replacement happened in previous reconcile
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1")
                .endMetadata()
                .build();
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1")
                .endMetadata()
                .build();

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(
                initialClusterCaKeySecret, initialClusterCaCertSecret,
                initialClientsCaKeySecret, initialClientsCaCertSecret)));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        // Kafka pods with old CA cert and key generation
        List<Pod> controllerPods = new ArrayList<>();
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-3", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-4", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-5", false, true, generationAnnotations));
        List<Pod> brokerPods = new ArrayList<>();
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-1", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-2", true, false, generationAnnotations));
        List<Pod> pods = new ArrayList<>(controllerPods);
        pods.addAll(brokerPods);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
        when(spsOps.getAsync(eq(NAMESPACE), eq(KafkaResources.zookeeperComponentName(NAME)))).thenReturn(Future.succeededFuture());
        StrimziPodSet controllerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-controller")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(controllerPods))
                .endSpec()
                .build();

        StrimziPodSet brokerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-broker")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(brokerPods))
                .endSpec()
                .build();

        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(controllerPodSet, brokerPodSet)));

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-cluster-entity-operator", deploymentWithName("my-cluster-entity-operator"));
        deps.put("my-cluster-cruise-control", deploymentWithName("my-cluster-cruise-control"));
        deps.put("my-cluster-kafka-exporter", deploymentWithName("my-cluster-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> Future.succeededFuture(deps.get(i.getArgument(1, String.class))));

        Checkpoint async = context.checkpoint();

        NewMockCaReconciler mockCaReconciler = new NewMockCaReconciler(reconciliation, KAFKA, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    // We don't handle this currently and rely on the rolling update later in the reconcile loop for Kafka
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());
                    async.flag();
                })));
    }

    @Test
    public void testStrimziManagedClientsCaCertRenewed(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        //Annotate Clients CA cert to force renewal
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true")
                .endMetadata()
                .build();

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(
                initialClusterCaKeySecret, initialClusterCaCertSecret,
                initialClientsCaKeySecret, initialClientsCaCertSecret)));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        // Kafka pods with old CA cert and key generation
        List<Pod> controllerPods = new ArrayList<>();
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-3", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-4", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-5", false, true, generationAnnotations));
        List<Pod> brokerPods = new ArrayList<>();
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-1", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-2", true, false, generationAnnotations));
        List<Pod> pods = new ArrayList<>(controllerPods);
        pods.addAll(brokerPods);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
        when(spsOps.getAsync(eq(NAMESPACE), eq(KafkaResources.zookeeperComponentName(NAME)))).thenReturn(Future.succeededFuture());
        StrimziPodSet controllerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-controller")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(controllerPods))
                .endSpec()
                .build();

        StrimziPodSet brokerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-broker")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(brokerPods))
                .endSpec()
                .build();

        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(controllerPodSet, brokerPodSet)));

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-cluster-entity-operator", deploymentWithName("my-cluster-entity-operator"));
        deps.put("my-cluster-cruise-control", deploymentWithName("my-cluster-cruise-control"));
        deps.put("my-cluster-kafka-exporter", deploymentWithName("my-cluster-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> Future.succeededFuture(deps.get(i.getArgument(1, String.class))));

        Checkpoint async = context.checkpoint();

        NewMockCaReconciler mockCaReconciler = new NewMockCaReconciler(reconciliation, KAFKA, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());
                    async.flag();
                })));
    }

    @Test
    public void testUserManagedClientsCaKeyReplaced(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        // Edit Clients CA key and cert to increment generation as though user has replaced CA key
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1")
                .endMetadata()
                .build();
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1")
                .endMetadata()
                .build();

        // Kafka brokers Secret with old annotation
        Secret kafkaBrokersSecret = kafkaBrokersSecretWithAnnotations(Map.of(
                Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(
                initialClusterCaKeySecret, initialClusterCaCertSecret,
                initialClientsCaKeySecret, initialClientsCaCertSecret,
                kafkaBrokersSecret)));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        // Kafka pods with old CA cert and key generation
        List<Pod> controllerPods = new ArrayList<>();
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-3", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-4", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-5", false, true, generationAnnotations));
        List<Pod> brokerPods = new ArrayList<>();
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-1", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-2", true, false, generationAnnotations));
        List<Pod> pods = new ArrayList<>(controllerPods);
        pods.addAll(brokerPods);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
        when(spsOps.getAsync(eq(NAMESPACE), eq(KafkaResources.zookeeperComponentName(NAME)))).thenReturn(Future.succeededFuture());
        StrimziPodSet controllerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-controller")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(controllerPods))
                .endSpec()
                .build();

        StrimziPodSet brokerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-broker")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(brokerPods))
                .endSpec()
                .build();

        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(controllerPodSet, brokerPodSet)));

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-cluster-entity-operator", deploymentWithName("my-cluster-entity-operator"));
        deps.put("my-cluster-cruise-control", deploymentWithName("my-cluster-cruise-control"));
        deps.put("my-cluster-kafka-exporter", deploymentWithName("my-cluster-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> Future.succeededFuture(deps.get(i.getArgument(1, String.class))));

        Checkpoint async = context.checkpoint();

        // Disable CA generation
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editClientsCa()
                        .withGenerateCertificateAuthority(false)
                    .endClientsCa()
                .endSpec()
                .build();
        NewMockCaReconciler mockCaReconciler = new NewMockCaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, aMapWithSize(6));
                    mockCaReconciler.kafkaRestartReasons.forEach((podName, restartReasons) -> {
                        assertThat("Restart reasons for pod " + podName, restartReasons.getReasons(), hasSize(1));
                        assertThat("Restart reasons for pod " + podName, restartReasons.contains(RestartReason.CLIENT_CA_CERT_KEY_REPLACED), is(true));
                    });
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());
                    async.flag();
                })));
    }

    @Test
    public void testUserManagedClientsCaCertRenewed(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        SecretOperator secretOps = supplier.secretOperations;
        CertificateAuthority certificateAuthority = new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        // Edit Clients CA cert to increment generation as though user has renewed CA cert
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1")
                .endMetadata()
                .build();

        // Kafka brokers Secret with old annotation
        Secret kafkaBrokersSecret = kafkaBrokersSecretWithAnnotations(Map.of(
                Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(
                initialClusterCaKeySecret, initialClusterCaCertSecret,
                initialClientsCaKeySecret, initialClientsCaCertSecret,
                kafkaBrokersSecret)));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        // Kafka pods with old CA cert and key generation
        List<Pod> controllerPods = new ArrayList<>();
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-3", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-4", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-5", false, true, generationAnnotations));
        List<Pod> brokerPods = new ArrayList<>();
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-1", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-2", true, false, generationAnnotations));
        List<Pod> pods = new ArrayList<>(controllerPods);
        pods.addAll(brokerPods);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
        when(spsOps.getAsync(eq(NAMESPACE), eq(KafkaResources.zookeeperComponentName(NAME)))).thenReturn(Future.succeededFuture());
        StrimziPodSet controllerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-controller")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(controllerPods))
                .endSpec()
                .build();

        StrimziPodSet brokerPodSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(NAME + "-broker")
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(brokerPods))
                .endSpec()
                .build();

        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(controllerPodSet, brokerPodSet)));

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-cluster-entity-operator", deploymentWithName("my-cluster-entity-operator"));
        deps.put("my-cluster-cruise-control", deploymentWithName("my-cluster-cruise-control"));
        deps.put("my-cluster-kafka-exporter", deploymentWithName("my-cluster-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> Future.succeededFuture(deps.get(i.getArgument(1, String.class))));

        Checkpoint async = context.checkpoint();

        // Disable CA generation
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editClientsCa()
                        .withGenerateCertificateAuthority(false)
                    .endClientsCa()
                .endSpec()
                .build();
        NewMockCaReconciler mockCaReconciler = new NewMockCaReconciler(reconciliation, kafka, new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                supplier, vertx, CERT_MANAGER, PASSWORD_GENERATOR);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    // When user is managing CA a cert renewal implies a key replacement
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, aMapWithSize(6));
                    mockCaReconciler.kafkaRestartReasons.forEach((podName, restartReasons) -> {
                        assertThat("Restart reasons for pod " + podName, restartReasons.getReasons(), hasSize(1));
                        assertThat("Restart reasons for pod " + podName, restartReasons.contains(RestartReason.CLIENT_CA_CERT_KEY_REPLACED), is(true));
                    });
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());
                    async.flag();
                })));
    }

    static class NewMockCaReconciler extends CaReconciler {
        Map<String, RestartReasons> kafkaRestartReasons = new HashMap<>();
        Map<String, String> deploymentRestartReasons = new HashMap<>();

        public NewMockCaReconciler(Reconciliation reconciliation, Kafka kafkaCr, ClusterOperatorConfig config, ResourceOperatorSupplier supplier, Vertx vertx, CertManager certManager, PasswordGenerator passwordGenerator) {
            super(reconciliation, kafkaCr, config, supplier, vertx, certManager, passwordGenerator);
        }

        @Override
        KafkaRoller createKafkaRoller(Set<NodeRef> nodes, TlsPemIdentity coTlsPemIdentity) {
            KafkaRoller mockKafkaRoller = mock(KafkaRoller.class);
            when(mockKafkaRoller.rollingRestart(any())).thenAnswer(i -> podOperator.listAsync(NAMESPACE, Labels.EMPTY)
                    .onSuccess(pods -> kafkaRestartReasons = pods.stream().collect(Collectors.toMap(
                            pod -> pod.getMetadata().getName(),
                            pod -> (RestartReasons) i.getArgument(0, Function.class).apply(pod))))
                    .mapEmpty());
            return mockKafkaRoller;
        }

        @Override
        Future<Void> rollDeploymentIfExists(String deploymentName, String reason) {
            return deploymentOperator.getAsync(reconciliation.namespace(), deploymentName)
                    .compose(dep -> {
                        if (dep != null) {
                            this.deploymentRestartReasons.put(deploymentName, reason);
                        }
                        return Future.succeededFuture();
                    });
        }
    }

    private static Pod podWithNameAndAnnotations(String name, boolean broker, boolean controller, Map<String, String> annotations) {
        return new PodBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withAnnotations(annotations)
                    .withLabels(Map.of(
                            Labels.STRIMZI_CLUSTER_LABEL, NAME,
                            Labels.STRIMZI_CONTROLLER_ROLE_LABEL, Boolean.toString(controller),
                            Labels.STRIMZI_BROKER_ROLE_LABEL, Boolean.toString(broker)
                            ))
                .endMetadata()
                .build();
    }

    private static Deployment deploymentWithName(String name) {
        return new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .build();
    }

    private static Secret kafkaBrokersSecretWithAnnotations(Map<String, String> annotations) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.kafkaSecretName(NAME))
                    .withAnnotations(annotations)
                .endMetadata()
                .build();
    }
}
