/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Volume;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationKeycloakBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaClusterOAuthTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private final static String NAMESPACE = "test";
    private final static String CLUSTER = "foo";
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
    private final static KafkaNodePool POOL_CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_MIXED = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("mixed")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(2)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_BROKERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("brokers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();

    //////////
    // Tests
    //////////

    @Test
    public void testGenerateDeploymentWithOAuthWithClientSecret() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .withAuth(
                                        new KafkaListenerAuthenticationOAuthBuilder()
                                                .withClientId("my-client-id")
                                                .withValidIssuerUri("http://valid-issuer")
                                                .withIntrospectionEndpointUri("http://introspection")
                                                .withNewClientSecret()
                                                .withSecretName("my-secret-secret")
                                                .withKey("my-secret-key")
                                                .endClientSecret()
                                                .build())
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.forEach(podSet -> PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElse(null), is(nullValue()));
            } else {
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
            }
        }));
    }

    @Test
    public void testGenerateDeploymentWithOAuthWithClientSecretAndTls() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .withAuth(
                                        new KafkaListenerAuthenticationOAuthBuilder()
                                                .withClientId("my-client-id")
                                                .withValidIssuerUri("http://valid-issuer")
                                                .withIntrospectionEndpointUri("http://introspection")
                                                .withNewClientSecret()
                                                .withSecretName("my-secret-secret")
                                                .withKey("my-secret-key")
                                                .endClientSecret()
                                                .withDisableTlsHostnameVerification(true)
                                                .withTlsTrustedCertificates(cert1, cert2, cert3)
                                                .build())
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.forEach(podSet -> PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            List<Volume> volumes = pod.getSpec().getVolumes();

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                // Env Vars
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_PLAIN_9092_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElse(null), is(nullValue()));

                // Volume mounts
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-plain-9092-first-certificate".equals(mount.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-plain-9092-second-certificate".equals(mount.getName())).findFirst().orElse(null), is(nullValue()));

                // Volumes
                assertThat(volumes.stream().filter(vol -> "oauth-plain-9092-first-certificate".equals(vol.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(volumes.stream().filter(vol -> "oauth-plain-9092-second-certificate".equals(vol.getName())).findFirst().orElse(null), is(nullValue()));
            } else {
                // Env Vars
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));
                assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_PLAIN_9092_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt"));

                // Volume mounts
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-plain-9092-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-plain-9092-certs/first-certificate"));
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-plain-9092-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-plain-9092-certs/second-certificate"));

                // Volumes
                assertThat(volumes.stream().filter(vol -> "oauth-plain-9092-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
                assertThat(volumes.stream().filter(vol -> "oauth-plain-9092-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            }
        }));
    }

    @Test
    public void testGenerateDeploymentWithOAuthEverywhere() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca2.crt")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withClientId("my-client-id")
                                                    .withValidIssuerUri("http://valid-issuer")
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .withNewClientSecret()
                                                    .withSecretName("my-secret-secret")
                                                    .withKey("my-secret-key")
                                                    .endClientSecret()
                                                    .withDisableTlsHostnameVerification(true)
                                                    .withTlsTrustedCertificates(cert1, cert2, cert3)
                                                    .build())
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withClientId("my-client-id")
                                                    .withValidIssuerUri("http://valid-issuer")
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .withNewClientSecret()
                                                    .withSecretName("my-secret-secret")
                                                    .withKey("my-secret-key")
                                                    .endClientSecret()
                                                    .withDisableTlsHostnameVerification(true)
                                                    .withTlsTrustedCertificates(cert1, cert2, cert3)
                                                    .build())
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.NODEPORT)
                                    .withTls(true)
                                    .withAuth(
                                            new KafkaListenerAuthenticationOAuthBuilder()
                                                    .withClientId("my-client-id")
                                                    .withValidIssuerUri("http://valid-issuer")
                                                    .withIntrospectionEndpointUri("http://introspection")
                                                    .withNewClientSecret()
                                                    .withSecretName("my-secret-secret")
                                                    .withKey("my-secret-key")
                                                    .endClientSecret()
                                                    .withDisableTlsHostnameVerification(true)
                                                    .withTlsTrustedCertificates(cert1, cert2, cert3)
                                                    .build())
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.forEach(podSet -> PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            List<Volume> volumes = pod.getSpec().getVolumes();

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                // Test Env Vars
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_TLS_9093_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_EXTERNAL_9094_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_PLAIN_9092_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_TLS_9093_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_EXTERNAL_9094_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElse(null), is(nullValue()));

                // Volume mounts
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-plain-9092-first-certificate".equals(mount.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-plain-9092-second-certificate".equals(mount.getName())).findFirst().orElse(null), is(nullValue()));

                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-tls-9093-first-certificate".equals(mount.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-tls-9093-second-certificate".equals(mount.getName())).findFirst().orElse(null), is(nullValue()));

                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-external-9094-first-certificate".equals(mount.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-external-9094-second-certificate".equals(mount.getName())).findFirst().orElse(null), is(nullValue()));

                // Volumes
                assertThat(volumes.stream().filter(vol -> "oauth-plain-9092-first-certificate".equals(vol.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(volumes.stream().filter(vol -> "oauth-plain-9092-second-certificate".equals(vol.getName())).findFirst().orElse(null), is(nullValue()));

                assertThat(volumes.stream().filter(vol -> "oauth-tls-9093-first-certificate".equals(vol.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(volumes.stream().filter(vol -> "oauth-tls-9093-second-certificate".equals(vol.getName())).findFirst().orElse(null), is(nullValue()));

                assertThat(volumes.stream().filter(vol -> "oauth-external-9094-first-certificate".equals(vol.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(volumes.stream().filter(vol -> "oauth-external-9094-second-certificate".equals(vol.getName())).findFirst().orElse(null), is(nullValue()));
            } else {
                // Test Env Vars
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_PLAIN_9092_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));

                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_TLS_9093_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_TLS_9093_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));

                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_EXTERNAL_9094_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getName(), is("my-secret-secret"));
                assertThat(cont.getEnv().stream().filter(var -> "STRIMZI_EXTERNAL_9094_OAUTH_CLIENT_SECRET".equals(var.getName())).findFirst().orElseThrow().getValueFrom().getSecretKeyRef().getKey(), is("my-secret-key"));

                assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_PLAIN_9092_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt"));
                assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_TLS_9093_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt"));
                assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_EXTERNAL_9094_OAUTH_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt;first-certificate/ca2.crt"));

                // Volume mounts
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-plain-9092-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-plain-9092-certs/first-certificate"));
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-plain-9092-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-plain-9092-certs/second-certificate"));

                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-tls-9093-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-tls-9093-certs/first-certificate"));
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-tls-9093-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-tls-9093-certs/second-certificate"));

                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-external-9094-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-external-9094-certs/first-certificate"));
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "oauth-external-9094-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/oauth-external-9094-certs/second-certificate"));

                // Volumes
                assertThat(volumes.stream().filter(vol -> "oauth-plain-9092-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
                assertThat(volumes.stream().filter(vol -> "oauth-plain-9092-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));

                assertThat(volumes.stream().filter(vol -> "oauth-tls-9093-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
                assertThat(volumes.stream().filter(vol -> "oauth-tls-9093-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));

                assertThat(volumes.stream().filter(vol -> "oauth-external-9094-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
                assertThat(volumes.stream().filter(vol -> "oauth-external-9094-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            }
        }));
    }

    @Test
    public void testGenerateDeploymentWithKeycloakAuthorization() {
        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("first-certificate")
                .withCertificate("ca.crt")
                .build();

        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("second-certificate")
                .withCertificate("tls.crt")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .withAuth(
                                        new KafkaListenerAuthenticationOAuthBuilder()
                                                .withClientId("my-client-id")
                                                .withValidIssuerUri("http://valid-issuer")
                                                .withIntrospectionEndpointUri("http://introspection")
                                                .withMaxSecondsWithoutReauthentication(3600)
                                                .withNewClientSecret()
                                                .withSecretName("my-secret-secret")
                                                .withKey("my-secret-key")
                                                .endClientSecret()
                                                .withDisableTlsHostnameVerification(true)
                                                .withTlsTrustedCertificates(cert1, cert2)
                                                .build())
                                .build())
                    .withAuthorization(
                            new KafkaAuthorizationKeycloakBuilder()
                                    .withClientId("my-client-id")
                                    .withTokenEndpointUri("http://token-endpoint-uri")
                                    .withDisableTlsHostnameVerification(true)
                                    .withDelegateToKafkaAcls(false)
                                    .withGrantsRefreshPeriodSeconds(90)
                                    .withGrantsRefreshPoolSize(4)
                                    .withTlsTrustedCertificates(cert1, cert2)
                                    .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        assertThat(podSets.size(), is(3));

        podSets.forEach(podSet -> PodSetUtils.podSetToPods(podSet).forEach(pod -> {
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            // Volume mounts
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "authz-keycloak-first-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/authz-keycloak-certs/first-certificate"));
            assertThat(cont.getVolumeMounts().stream().filter(mount -> "authz-keycloak-second-certificate".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.TRUSTED_CERTS_BASE_VOLUME_MOUNT + "/authz-keycloak-certs/second-certificate"));

            // Environment variable
            assertThat(cont.getEnv().stream().filter(e -> "STRIMZI_KEYCLOAK_AUTHZ_TRUSTED_CERTS".equals(e.getName())).findFirst().orElseThrow().getValue(), is("first-certificate/ca.crt;second-certificate/tls.crt"));

            // Volumes
            List<Volume> volumes = pod.getSpec().getVolumes();
            assertThat(volumes.stream().filter(vol -> "authz-keycloak-first-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
            assertThat(volumes.stream().filter(vol -> "authz-keycloak-second-certificate".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().isEmpty(), is(true));
        }));
    }

    @Test
    public void testGenerateDeploymentWithKeycloakAuthorizationMissingOAuthListeners() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withAuthorization(
                            new KafkaAuthorizationKeycloakBuilder().build())
                    .endKafka()
                    .endSpec()
                    .build();

            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        });
    }
}
