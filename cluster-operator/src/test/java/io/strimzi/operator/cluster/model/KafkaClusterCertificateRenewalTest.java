/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaClusterCertificateRenewalTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private final static String NAMESPACE = "test";
    private final static String CLUSTER = "foo";
    private final static String POD_0 = CLUSTER + "-pod-0";
    private final static String POD_1 = CLUSTER + "-pod-1";
    private final static String POD_2 = CLUSTER + "-pod-2";
    private final static String DUMMY_CERT = "DUMMY_CERT";
    private final static String DUMMY_KEY = "DUMMY_KEY";
    private final static String EXPIRED_DUMMY_CERT = "EXPIRED_DUMMY_CERT";

    private final static Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                            new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build())
                .endKafka()
            .endSpec()
            .build();
    private final static KafkaNodePool NODES = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("pod")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
            .endSpec()
            .build();

    private static final List<KafkaPool> POOLS = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(NODES), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
    private final static KafkaCluster KC = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, POOLS, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

    @Test
    public void renewalOfCertificatesWithNewNodesOutsideWindow() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Secret pod0CertSecret = createCertSecret(POD_0, EXPIRED_DUMMY_CERT, DUMMY_KEY);
        Secret pod1CertSecret = createCertSecret(POD_1, EXPIRED_DUMMY_CERT, DUMMY_KEY);

        KC.generateCertificatesSecrets(
                mockedCa,
                List.of(pod0CertSecret, pod1CertSecret),
                Map.of(),
                Set.of(),
                Map.of(),
                false
        ).whenComplete(((secrets, throwable) -> {
            assertThat(throwable, is(nullValue()));

            assertThat(secrets.size(), is(3));
            Map<String, Secret> secretMap = secrets.stream().collect(Collectors.toMap(secret -> secret.getMetadata().getName(), secret -> secret));

            assertThat(secretMap.get(POD_0), is(notNullValue()));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_0).getData().get(POD_0 + ".crt")), is(EXPIRED_DUMMY_CERT));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_0).getData().get(POD_0 + ".key")), is(DUMMY_KEY));

            assertThat(secretMap.get(POD_1), is(notNullValue()));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_1).getData().get(POD_1 + ".crt")), is(EXPIRED_DUMMY_CERT));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_1).getData().get(POD_1 + ".key")), is(DUMMY_KEY));

            assertThat(secretMap.get(POD_2), is(notNullValue()));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_2).getData().get(POD_2 + ".crt")), is("new-cert-" + POD_2));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_2).getData().get(POD_2 + ".key")), is("new-key-" + POD_2));

        })).toCompletableFuture().join();
    }

    @Test
    public void nosRenewalOfCertificatesWithScaleUp() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Secret pod0CertSecret = createCertSecret(POD_0, DUMMY_CERT, DUMMY_KEY);

        KC.generateCertificatesSecrets(
                mockedCa,
                List.of(pod0CertSecret),
                Map.of(),
                Set.of(),
                Map.of(),
                true
        ).whenComplete(((secrets, throwable) -> {
            assertThat(throwable, is(nullValue()));

            assertThat(secrets.size(), is(3));
            Map<String, Secret> secretMap = secrets.stream().collect(Collectors.toMap(secret -> secret.getMetadata().getName(), secret -> secret));

            assertThat(secretMap.get(POD_0), is(notNullValue()));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_0).getData().get(POD_0 + ".crt")), is(DUMMY_CERT));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_0).getData().get(POD_0 + ".key")), is(DUMMY_KEY));

            assertThat(secretMap.get(POD_1), is(notNullValue()));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_1).getData().get(POD_1 + ".crt")), is("new-cert-" + POD_1));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_1).getData().get(POD_1 + ".key")), is("new-key-" + POD_1));

            assertThat(secretMap.get(POD_2), is(notNullValue()));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_2).getData().get(POD_2 + ".crt")), is("new-cert-" + POD_2));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_2).getData().get(POD_2 + ".key")), is("new-key-" + POD_2));

        })).toCompletableFuture().join();
    }

    @Test
    public void noRenewalOfCertificatesWithScaleUpInTheMiddle() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Secret pod0CertSecret = createCertSecret(POD_0, DUMMY_CERT, DUMMY_KEY);
        Secret pod2CertSecret = createCertSecret(POD_2, DUMMY_CERT, DUMMY_KEY);

        KC.generateCertificatesSecrets(
                mockedCa,
                List.of(pod0CertSecret, pod2CertSecret),
                Map.of(),
                Set.of(),
                Map.of(),
                true
        ).whenComplete(((secrets, throwable) -> {
            assertThat(throwable, is(nullValue()));

            assertThat(secrets.size(), is(3));
            Map<String, Secret> secretMap = secrets.stream().collect(Collectors.toMap(secret -> secret.getMetadata().getName(), secret -> secret));

            assertThat(secretMap.get(POD_0), is(notNullValue()));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_0).getData().get(POD_0 + ".crt")), is(DUMMY_CERT));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_0).getData().get(POD_0 + ".key")), is(DUMMY_KEY));

            assertThat(secretMap.get(POD_1), is(notNullValue()));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_1).getData().get(POD_1 + ".crt")), is("new-cert-" + POD_1));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_1).getData().get(POD_1 + ".key")), is("new-key-" + POD_1));

            assertThat(secretMap.get(POD_2), is(notNullValue()));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_2).getData().get(POD_2 + ".crt")), is(DUMMY_CERT));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_2).getData().get(POD_2 + ".key")), is(DUMMY_KEY));

        })).toCompletableFuture().join();
    }

    @Test
    public void noRenewalOfCertificatesScaleDown() {
        MockedClusterCa mockedCa = new MockedClusterCa();

        Secret pod0CertSecret = createCertSecret(POD_0, DUMMY_CERT, DUMMY_KEY);
        Secret pod1CertSecret = createCertSecret(POD_1, DUMMY_CERT, DUMMY_KEY);
        Secret pod2CertSecret = createCertSecret(POD_2, DUMMY_CERT, DUMMY_KEY);
        Secret pod3CertSecret = createCertSecret("foo-pod-3", DUMMY_CERT, DUMMY_KEY);

        KC.generateCertificatesSecrets(
                mockedCa,
                List.of(pod0CertSecret, pod1CertSecret, pod2CertSecret, pod3CertSecret),
                Map.of(),
                Set.of(),
                Map.of(),
                true
        ).whenComplete(((secrets, throwable) -> {
            assertThat(throwable, is(nullValue()));

            assertThat(secrets.size(), is(3));
            Map<String, Secret> secretMap = secrets.stream().collect(Collectors.toMap(secret -> secret.getMetadata().getName(), secret -> secret));

            assertThat(secretMap.get(POD_0), is(notNullValue()));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_0).getData().get(POD_0 + ".crt")), is(DUMMY_CERT));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_0).getData().get(POD_0 + ".key")), is(DUMMY_KEY));

            assertThat(secretMap.get(POD_1), is(notNullValue()));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_1).getData().get(POD_1 + ".crt")), is(DUMMY_CERT));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_1).getData().get(POD_1 + ".key")), is(DUMMY_KEY));

            assertThat(secretMap.get(POD_2), is(notNullValue()));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_2).getData().get(POD_2 + ".crt")), is(DUMMY_CERT));
            assertThat(Util.decodeFromBase64(secretMap.get(POD_2).getData().get(POD_2 + ".key")), is(DUMMY_KEY));

        })).toCompletableFuture().join();
    }

    public static class MockedClusterCa extends Ca {

        public MockedClusterCa() {
            super(Reconciliation.DUMMY_RECONCILIATION, CaRole.CLUSTER_CA, null, null, CaConfig.createDefault());
        }

        @Override
        public CompletionStage<CertAndKey> maybeCopyOrGenerateServerCerts(Reconciliation reconciliation, String commonName, Subject subject, CertAndKey existingCertAndKey, boolean isMaintenanceTimeWindowsSatisfied, boolean includeCaChain) {
            if (existingCertAndKey != null) {
                if (Arrays.equals(existingCertAndKey.cert(), DUMMY_CERT.getBytes(StandardCharsets.UTF_8)) || (Arrays.equals(existingCertAndKey.cert(), EXPIRED_DUMMY_CERT.getBytes(StandardCharsets.UTF_8)) && !isMaintenanceTimeWindowsSatisfied)) {
                    // We either have a valid cert, or we have an expired cert but are outside maintenance window - return existing cert
                    return CompletableFuture.completedFuture(existingCertAndKey);
                }
            }
            return CompletableFuture.completedFuture(new CertAndKey(("new-key-" + commonName).getBytes(StandardCharsets.UTF_8), ("new-cert-" + commonName).getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public CompletionStage<CertAndKey> maybeCopyOrGenerateClientCert(Reconciliation reconciliation, String commonName, CertAndKey existingCertAndKey, boolean isMaintenanceTimeWindowsSatisfied) {
            return null;
        }

        @Override
        protected boolean removeCerts(Map<String, String> newData, Predicate<Map.Entry<String, String>> predicate) {
            return false;
        }

        @Override
        public void maybeDeleteOldCerts() {

        }

        @Override
        protected int initCaKeyGeneration(Secret caKeySecret, Secret caCertSecret) {
            return 0;
        }
    }

    public static Secret createCertSecret(String secretName, String caCert, String caKey) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(NAMESPACE)
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION, "0")
                    .withLabels(Labels.forStrimziCluster(CLUSTER).withStrimziKind(Kafka.RESOURCE_KIND).toMap())
                .endMetadata()
                .addToData(secretName + ".crt", Util.encodeToBase64(caCert))
                .addToData(secretName + ".key", Util.encodeToBase64(caKey))
                .build();
    }
}
