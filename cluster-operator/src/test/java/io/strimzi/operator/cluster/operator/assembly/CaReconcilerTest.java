/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.api.ResourceAnnotations;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.certs.CertAndKey;
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
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.vertx.core.Future;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.model.Ca.CA_CRT;
import static io.strimzi.operator.common.model.Ca.CA_STORE;
import static io.strimzi.operator.common.model.Ca.CA_STORE_PASSWORD;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class CaReconcilerTest {
    private static final String NAMESPACE = "test";
    private static final String NAME = "my-cluster";
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
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

    private WorkerExecutor sharedWorkerExecutor;
    private ResourceOperatorSupplier supplier;

    @BeforeEach
    public void setup(Vertx vertx) {
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
        supplier = ResourceUtils.supplierWithMocks(false);
    }

    @AfterEach
    public void teardown() {
        sharedWorkerExecutor.close();
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

    @Test
    public void testOldClusterCaCertsGetsRemovedAuto(Vertx vertx, VertxTestContext context)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);
        CertificateAuthority certificateAuthority = getCertificateAuthority();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        Secret initialClusterCaKeySecret = clusterCaSecrets.get(0);
        Secret initialClusterCaCertSecret = clusterCaSecrets.get(1);

        // add an "old" certificate to the secret with format ca-YYYY-MM-DDTHH-MM-SSZ
        // for ease re-use existing ca.crt file contents
        String oldCertAlias = "ca-2025-08-06T09-00-00Z.crt";
        initialClusterCaCertSecret.getData().put(oldCertAlias, initialClusterCaCertSecret.getData().get(CA_CRT));

        // ... and to the related truststore
        Path certFile = Files.createTempFile("tls", "-cert");
        certFile.toFile().deleteOnExit();
        Path trustStoreFile = Files.createTempFile("tls", "-truststore");
        trustStoreFile.toFile().deleteOnExit();
        Files.write(certFile, Util.decodeBytesFromBase64(initialClusterCaCertSecret.getData().get(oldCertAlias)));
        Files.write(trustStoreFile, Util.decodeBytesFromBase64(initialClusterCaCertSecret.getData().get(CA_STORE)));
        String trustStorePassword = Util.decodeFromBase64(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD));
        CERT_MANAGER.addCertToTrustStore(certFile.toFile(), oldCertAlias, trustStoreFile.toFile(), trustStorePassword);
        initialClusterCaCertSecret.getData().put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile)));

        // Check it was added correctly
        KeyStore initialClusterCaTrustStore = KeyStore.getInstance("PKCS12");
        initialClusterCaTrustStore.load(new ByteArrayInputStream(Util.decodeBytesFromBase64(initialClusterCaCertSecret.getData().get(CA_STORE))),
                Util.decodeFromBase64(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)).toCharArray()
        );
        assertThat(initialClusterCaTrustStore.isCertificateEntry(oldCertAlias), is(true));

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        Secret initialClientsCaKeySecret = clientsCaSecrets.get(0);
        Secret initialClientsCaCertSecret = clientsCaSecrets.get(1);

        // add an "old" certificate to the secret with format ca-YYYY-MM-DDTHH-MM-SSZ
        // for ease re-use existing ca.crt file contents
        initialClientsCaCertSecret.getData().put(oldCertAlias, initialClientsCaCertSecret.getData().get(CA_CRT));

        // ... and to the related truststore
        certFile = Files.createTempFile("tls", "-cert");
        certFile.toFile().deleteOnExit();
        Files.write(certFile, Util.decodeBytesFromBase64(initialClientsCaCertSecret.getData().get(oldCertAlias)));
        trustStoreFile = Files.createTempFile("tls", "-truststore");
        trustStoreFile.toFile().deleteOnExit();
        Files.write(trustStoreFile, Util.decodeBytesFromBase64(initialClientsCaCertSecret.getData().get(CA_STORE)));
        trustStorePassword = Util.decodeFromBase64(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD));
        CERT_MANAGER.addCertToTrustStore(certFile.toFile(), oldCertAlias, trustStoreFile.toFile(), trustStorePassword);
        initialClientsCaCertSecret.getData().put(CA_STORE, Base64.getEncoder().encodeToString(Files.readAllBytes(trustStoreFile)));

        // Check it was added correctly
        KeyStore initialClientsCaTrustStore = KeyStore.getInstance("PKCS12");
        initialClientsCaTrustStore.load(new ByteArrayInputStream(Util.decodeBytesFromBase64(initialClientsCaCertSecret.getData().get(CA_STORE))),
                Util.decodeFromBase64(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)).toCharArray()
        );
        assertThat(initialClientsCaTrustStore.isCertificateEntry(oldCertAlias), is(true));

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        Pod controllerPod = podWithNameAndAnnotations("my-cluster-controllers-1", false, true, generationAnnotations);
        Pod brokerPod = podWithNameAndAnnotations("my-cluster-brokers-0", true, false, generationAnnotations);
        initTrustRolloutTestMocks(supplier,
                List.of(initialClusterCaCertSecret, initialClusterCaKeySecret,
                        initialClientsCaCertSecret, initialClientsCaKeySecret),
                List.of(controllerPod),
                List.of(brokerPod));

        Checkpoint async = context.checkpoint();
        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
                    ArgumentCaptor<Secret> clientsCaCert = ArgumentCaptor.forClass(Secret.class);

                    // Cluster CA should be reconciled twice, once initially, then when removing the old cert. Clients CA is only reconciled once
                    verify(supplier.secretOperations, times(2)).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture());
                    verify(supplier.secretOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), clientsCaCert.capture());

                    //getValue() returns the latest captured value
                    Map<String, String> clusterCaCertData = clusterCaCert.getValue().getData();
                    assertThat(clusterCaCertData, aMapWithSize(3));
                    assertThat(clusterCaCertData.get(CA_CRT), is(initialClusterCaCertSecret.getData().get(CA_CRT)));
                    assertThat(clusterCaCertData.get(CA_STORE_PASSWORD), is(initialClusterCaCertSecret.getData().get(CA_STORE_PASSWORD)));

                    X509Certificate x509clusterCaCert = (X509Certificate) CertificateFactory.getInstance("X.509")
                            .generateCertificate(new ByteArrayInputStream(Util.decodeBytesFromBase64(clusterCaCertData.get(CA_CRT))));
                    KeyStore clusterCaTrustStore = KeyStore.getInstance("PKCS12");
                    clusterCaTrustStore.load(new ByteArrayInputStream(Util.decodeBytesFromBase64(clusterCaCertData.get(CA_STORE))),
                            Util.decodeFromBase64(clusterCaCertData.get(CA_STORE_PASSWORD)).toCharArray()
                    );
                    assertThat((X509Certificate) clusterCaTrustStore.getCertificate(CA_CRT), is(x509clusterCaCert));
                    assertThat(clusterCaTrustStore.isCertificateEntry(oldCertAlias), is(false));

                    // Clients CA cert doesn't have old CA certs removed automatically
                    Map<String, String> clientsCaCertData = clientsCaCert.getValue().getData();
                    assertThat(clientsCaCertData, aMapWithSize(4));
                    assertThat(clientsCaCertData.get(CA_CRT), is(initialClientsCaCertSecret.getData().get(CA_CRT)));
                    assertThat(clientsCaCertData.get(CA_STORE_PASSWORD), is(initialClientsCaCertSecret.getData().get(CA_STORE_PASSWORD)));
                    assertThat(clientsCaCertData.get(oldCertAlias), is(initialClientsCaCertSecret.getData().get(CA_CRT)));

                    X509Certificate x509clientsCaCert = (X509Certificate) CertificateFactory.getInstance("X.509")
                            .generateCertificate(new ByteArrayInputStream(Util.decodeBytesFromBase64(clientsCaCertData.get(CA_CRT))));
                    KeyStore clientsCaTrustStore = KeyStore.getInstance("PKCS12");
                    clientsCaTrustStore.load(new ByteArrayInputStream(Util.decodeBytesFromBase64(clientsCaCertData.get(CA_STORE))),
                            Util.decodeFromBase64(clientsCaCertData.get(CA_STORE_PASSWORD)).toCharArray()
                    );
                    assertThat((X509Certificate) clientsCaTrustStore.getCertificate(CA_CRT), is(x509clientsCaCert));
                    assertThat(clientsCaTrustStore.isCertificateEntry(oldCertAlias), is(true));
                    async.flag();
                })));
    }

    @Test
    public void testStrimziManagedClusterCaKeyReplaced(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = getCertificateAuthority();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        //Annotate Cluster CA key to force replacement
        Secret clusterCaKeySecret = clusterCaSecrets.get(0).edit()
                .editMetadata()
                    .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_REPLACE, "true")
                .endMetadata()
                .build();

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);

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

        initTrustRolloutTestMocks(supplier,
                List.of(clusterCaKeySecret, clusterCaSecrets.get(1),
                        clientsCaSecrets.get(0), clientsCaSecrets.get(1)),
                controllerPods,
                brokerPods);

        Checkpoint async = context.checkpoint(2);
        when(supplier.strimziPodSetOperator.batchReconcile(any(), eq(NAMESPACE), any(), any(Labels.class))).thenAnswer(i -> {
            List<StrimziPodSet> podSets = i.getArgument(2);
            context.verify(() -> {
                assertThat(podSets, hasSize(2));
                List<Pod> returnedPods = podSets
                        .stream()
                        .flatMap(podSet -> PodSetUtils.podSetToPods(podSet).stream())
                        .toList();
                for (Pod pod : returnedPods) {
                    Map<String, String> podAnnotations = pod.getMetadata().getAnnotations();
                    // Expect that the CA key generation was updated. CA cert generations are updated by component reconcilers
                    assertThat(podAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                    assertThat(podAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "1"));
                    assertThat(podAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
                }
            });
            async.flag();
            return Future.succeededFuture();
        });

        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
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
        CertificateAuthority certificateAuthority = getCertificateAuthority();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        // Edit Cluster CA key and cert to increment generation as though replacement happened in previous reconcile
        Secret clusterCaKeySecret = clusterCaSecrets.get(0).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1")
                .endMetadata()
                .build();
        Secret clusterCaCertSecret = clusterCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1")
                .endMetadata()
                .build();

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);

        Map<String, String> generationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");
        Map<String, String> updatedGenerationAnnotations =
                Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0",
                        Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "1",
                        Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0");

        // Kafka pods with old CA cert and key generation
        List<Pod> controllerPods = new ArrayList<>();
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-3", false, true, generationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-4", false, true, updatedGenerationAnnotations));
        controllerPods.add(podWithNameAndAnnotations("my-cluster-controllers-5", false, true, generationAnnotations));
        List<Pod> brokerPods = new ArrayList<>();
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-0", true, false, updatedGenerationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-1", true, false, generationAnnotations));
        brokerPods.add(podWithNameAndAnnotations("my-cluster-brokers-2", true, false, generationAnnotations));

        initTrustRolloutTestMocks(supplier,
                List.of(clusterCaKeySecret, clusterCaCertSecret,
                        clientsCaSecrets.get(0), clientsCaSecrets.get(1)),
                controllerPods,
                brokerPods
        );

        Checkpoint async = context.checkpoint(2);
        when(supplier.strimziPodSetOperator.batchReconcile(any(), eq(NAMESPACE), any(), any(Labels.class))).thenAnswer(i -> {
            List<StrimziPodSet> podSets = i.getArgument(2);
            context.verify(() -> {
                assertThat(podSets, hasSize(2));
                List<Pod> returnedPods = podSets
                        .stream()
                        .flatMap(podSet -> PodSetUtils.podSetToPods(podSet).stream())
                        .toList();
                for (Pod pod : returnedPods) {
                    Map<String, String> podAnnotations = pod.getMetadata().getAnnotations();
                    // Expect that the CA key generation was updated. CA cert generations are updated by component reconcilers
                    assertThat(podAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                    assertThat(podAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "1"));
                    assertThat(podAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
                }
            });
            async.flag();
            return Future.succeededFuture();
        });

        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, aMapWithSize(6));
                    mockCaReconciler.kafkaRestartReasons.forEach((podName, restartReasons) -> {
                        if ("my-cluster-controllers-4".equals(podName) || "my-cluster-brokers-0".equals(podName)) {
                            assertThat("Pod " + podName + " should not be restarted", restartReasons.getReasons(), empty());
                        } else {
                            assertThat("Restart reasons for pod " + podName, restartReasons.getReasons(), hasSize(1));
                            assertThat("Restart reasons for pod " + podName, restartReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED), is(true));
                        }
                    });
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, aMapWithSize(3));
                    mockCaReconciler.deploymentRestartReasons.forEach((deploymentName, restartReason) ->
                            assertThat("Deployment restart reason for " + deploymentName, restartReason.equals(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED.getDefaultNote()), is(true)));
                    async.flag();
                })));
    }

    @Test
    public void testStrimziManagedClusterCaCertRenewed(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = getCertificateAuthority();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        //Annotate Cluster CA cert to force renewal
        Secret clusterCaCertSecret = clusterCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true")
                .endMetadata()
                .build();

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);

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

        initTrustRolloutTestMocks(supplier,
                List.of(clusterCaSecrets.get(0), clusterCaCertSecret,
                        clientsCaSecrets.get(0), clientsCaSecrets.get(1)),
                controllerPods,
                brokerPods);

        Checkpoint async = context.checkpoint();

        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
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
        CertificateAuthority certificateAuthority = getCertificateAuthority();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        // Edit Cluster CA key and cert to increment generation as though user has replaced CA key
        Secret clusterCaKeySecret = clusterCaSecrets.get(0).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1")
                .endMetadata()
                .build();
        Secret clusterCaCertSecret = clusterCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1")
                .endMetadata()
                .build();

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);

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

        initTrustRolloutTestMocks(supplier,
                List.of(clusterCaKeySecret, clusterCaCertSecret,
                        clientsCaSecrets.get(0), clientsCaSecrets.get(1)),
                controllerPods,
                brokerPods);

        Checkpoint async = context.checkpoint(2);
        when(supplier.strimziPodSetOperator.batchReconcile(any(), eq(NAMESPACE), any(), any(Labels.class))).thenAnswer(i -> {
            List<StrimziPodSet> podSets = i.getArgument(2);
            context.verify(() -> {
                assertThat(podSets, hasSize(2));
                List<Pod> returnedPods = podSets
                        .stream()
                        .flatMap(podSet -> PodSetUtils.podSetToPods(podSet).stream())
                        .toList();
                for (Pod pod : returnedPods) {
                    Map<String, String> podAnnotations = pod.getMetadata().getAnnotations();
                    // Expect that the CA key generation was updated. CA cert generations are updated by component reconcilers
                    assertThat(podAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
                    assertThat(podAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "1"));
                    assertThat(podAnnotations, hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
                }
            });
            async.flag();
            return Future.succeededFuture();
        });

        // Disable CA generation
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editClusterCa()
                        .withGenerateCertificateAuthority(false)
                    .endClusterCa()
                .endSpec()
                .build();
        MockCaReconciler mockCaReconciler = new MockCaReconciler(kafka, supplier, vertx);
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
        CertificateAuthority certificateAuthority = getCertificateAuthority();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);
        // Edit Cluster CA cert to increment generation as though user has renewed CA cert
        Secret clusterCaCertSecret = clusterCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1")
                .endMetadata()
                .build();

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);

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

        initTrustRolloutTestMocks(supplier,
                List.of(clusterCaSecrets.get(0), clusterCaCertSecret,
                        clientsCaSecrets.get(0), clientsCaSecrets.get(1)),
                controllerPods,
                brokerPods);

        Checkpoint async = context.checkpoint();

        // Disable CA generation
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editClusterCa()
                        .withGenerateCertificateAuthority(false)
                    .endClusterCa()
                .endSpec()
                .build();
        MockCaReconciler mockCaReconciler = new MockCaReconciler(kafka, supplier, vertx);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());
                    async.flag();
                })));
    }

    @Test
    public void testStrimziManagedClientsCaKeyReplaced(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = getCertificateAuthority();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        //Annotate Clients CA key to force replacement
        Secret clientsCaKeySecret = clientsCaSecrets.get(0).edit()
                .editMetadata()
                    .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_REPLACE, "true")
                .endMetadata()
                .build();

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

        initTrustRolloutTestMocks(supplier,
                List.of(clusterCaSecrets.get(0), clusterCaSecrets.get(1),
                        clientsCaKeySecret, clientsCaSecrets.get(1)),
                controllerPods,
                brokerPods);

        Checkpoint async = context.checkpoint();

        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    // We rely on KafkaReconciler to roll pods for ClientsCa renewal
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());
                    async.flag();
                })));
    }

    // Strimzi Clients CA key replaced in previous reconcile and some pods already rolled
    @Test
    public void testStrimziManagedClientsCaKeyReplacedPreviously(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = getCertificateAuthority();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        // Edit Clients CA key and cert to increment generation as though replacement happened in previous reconcile
        Secret clientsCaKeySecret = clientsCaSecrets.get(0).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1")
                .endMetadata()
                .build();
        Secret clientsCaCertSecret = clientsCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1")
                .endMetadata()
                .build();

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

        initTrustRolloutTestMocks(supplier,
                List.of(clusterCaSecrets.get(0), clusterCaSecrets.get(1),
                        clientsCaKeySecret, clientsCaCertSecret),
                controllerPods,
                brokerPods);

        Checkpoint async = context.checkpoint();

        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
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
        CertificateAuthority certificateAuthority = getCertificateAuthority();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        //Annotate Clients CA cert to force renewal
        Secret clientsCaCertSecret = clientsCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_RENEW, "true")
                .endMetadata()
                .build();

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

        initTrustRolloutTestMocks(supplier,
                List.of(clusterCaSecrets.get(0), clusterCaSecrets.get(1),
                        clientsCaSecrets.get(0), clientsCaCertSecret),
                controllerPods,
                brokerPods);

        Checkpoint async = context.checkpoint();

        MockCaReconciler mockCaReconciler = new MockCaReconciler(KAFKA, supplier, vertx);
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
        CertificateAuthority certificateAuthority = getCertificateAuthority();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        // Edit Clients CA key and cert to increment generation as though user has replaced CA key
        Secret clientsCaKeySecret = clientsCaSecrets.get(0).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION, "1")
                .endMetadata()
                .build();
        Secret clientsCaCertSecret = clientsCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1")
                .endMetadata()
                .build();

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

        initTrustRolloutTestMocks(supplier,
                List.of(clusterCaSecrets.get(0), clusterCaSecrets.get(1),
                        clientsCaKeySecret, clientsCaCertSecret),
                controllerPods,
                brokerPods);

        Checkpoint async = context.checkpoint();

        // Disable CA generation
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editClientsCa()
                        .withGenerateCertificateAuthority(false)
                    .endClientsCa()
                .endSpec()
                .build();
        MockCaReconciler mockCaReconciler = new MockCaReconciler(kafka, supplier, vertx);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    // We rely on KafkaReconciler to roll pods for ClientsCa renewal
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());
                    async.flag();
                })));
    }

    @Test
    public void testUserManagedClientsCaCertRenewed(Vertx vertx, VertxTestContext context) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        CertificateAuthority certificateAuthority = getCertificateAuthority();

        List<Secret> clusterCaSecrets = initialClusterCaSecrets(certificateAuthority);

        List<Secret> clientsCaSecrets = initialClientsCaSecrets(certificateAuthority);
        // Edit Clients CA cert to increment generation as though user has renewed CA cert
        Secret clientsCaCertSecret = clientsCaSecrets.get(1).edit()
                .editMetadata()
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "1")
                .endMetadata()
                .build();

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

        initTrustRolloutTestMocks(supplier,
                List.of(clusterCaSecrets.get(0), clusterCaSecrets.get(1),
                        clientsCaSecrets.get(0), clientsCaCertSecret),
                controllerPods,
                brokerPods);

        Checkpoint async = context.checkpoint();

        // Disable CA generation
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editClientsCa()
                        .withGenerateCertificateAuthority(false)
                    .endClientsCa()
                .endSpec()
                .build();
        MockCaReconciler mockCaReconciler = new MockCaReconciler(kafka, supplier, vertx);
        mockCaReconciler
                .reconcile(Clock.systemUTC())
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
                    assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());
                    async.flag();
                })));
    }

    private void initTrustRolloutTestMocks(ResourceOperatorSupplier supplier,
                                           List<Secret> secrets,
                                           List<Pod> controllerPods,
                                           List<Pod> brokerPods) {
        SecretOperator secretOps = supplier.secretOperations;

        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(secrets));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(Future.succeededFuture());

        PodOperator mockPodOps = supplier.podOperations;
        List<Pod> pods = new ArrayList<>(controllerPods);
        pods.addAll(brokerPods);
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
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
    }

    static class MockCaReconciler extends CaReconciler {
        Map<String, RestartReasons> kafkaRestartReasons = new HashMap<>();
        Map<String, String> deploymentRestartReasons = new HashMap<>();

        public MockCaReconciler(Kafka kafkaCr, ResourceOperatorSupplier supplier, Vertx vertx) {
            super(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME),
                    kafkaCr,
                    new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                    supplier,
                    vertx,
                    CERT_MANAGER,
                    PASSWORD_GENERATOR
            );
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
        Future<Void> rollDeploymentIfExists(String deploymentName, RestartReason reason) {
            return deploymentOperator.getAsync(reconciliation.namespace(), deploymentName)
                    .compose(dep -> {
                        if (dep != null) {
                            this.deploymentRestartReasons.put(deploymentName, reason.getDefaultNote());
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

    private static CertificateAuthority getCertificateAuthority() {
        return new CertificateAuthorityBuilder()
                .withValidityDays(100)
                .withRenewalDays(10)
                .withGenerateCertificateAuthority(true)
                .build();
    }
}
