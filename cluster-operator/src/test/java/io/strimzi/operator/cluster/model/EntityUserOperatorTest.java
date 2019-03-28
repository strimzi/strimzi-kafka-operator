/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBinding;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.EntityUserOperatorSpecBuilder;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class EntityUserOperatorTest {

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final InlineLogging userOperatorLogging = new InlineLogging();
    {
        userOperatorLogging.setLoggers(Collections.singletonMap("user-operator.root.logger", "OFF"));
    }

    private final String uoWatchedNamespace = "my-user-namespace";
    private final String uoImage = "my-user-operator-image";
    private final int uoReconciliationInterval = 90;
    private final int uoZookeeperSessionTimeout = 20;

    private final EntityUserOperatorSpec entityUserOperatorSpec = new EntityUserOperatorSpecBuilder()
            .withWatchedNamespace(uoWatchedNamespace)
            .withImage(uoImage)
            .withReconciliationIntervalSeconds(uoReconciliationInterval)
            .withZookeeperSessionTimeoutSeconds(uoZookeeperSessionTimeout)
            .withLogging(userOperatorLogging)
            .build();

    private final EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
            .withUserOperator(entityUserOperatorSpec)
            .build();

    private final Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                    .endSpec()
                    .build();

    private final EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(resource);

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_ZOOKEEPER_CONNECT).withValue(String.format("%s:%d", "localhost", EntityUserOperatorSpec.DEFAULT_ZOOKEEPER_PORT)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_WATCHED_NAMESPACE).withValue(uoWatchedNamespace).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_RESOURCE_LABELS).withValue(ModelUtils.defaultResourceLabels(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS).withValue(String.valueOf(uoReconciliationInterval * 1000)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS).withValue(String.valueOf(uoZookeeperSessionTimeout * 1000)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_KEY_SECRET_NAME).withValue(KafkaCluster.clientsCaKeySecretName(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_CERT_SECRET_NAME).withValue(KafkaCluster.clientsCaCertSecretName(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_NAMESPACE).withValue(namespace).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(KafkaCluster.DEFAULT_STRIMZI_GC_LOG_ENABED).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_VALIDITY).withValue(Integer.toString(CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_RENEWAL).withValue(Integer.toString(CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS)).build());
        return expected;
    }

    @Test
    public void testEnvVars()   {
        Assert.assertEquals(getExpectedEnvVars(), entityUserOperator.getEnvVars());
    }

    @Test
    public void testFromCrd() {
        assertEquals(namespace, entityUserOperator.namespace);
        assertEquals(cluster, entityUserOperator.cluster);
        assertEquals(uoImage, entityUserOperator.image);
        assertEquals(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_DELAY, entityUserOperator.readinessInitialDelay);
        assertEquals(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT, entityUserOperator.readinessTimeout);
        assertEquals(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_DELAY, entityUserOperator.livenessInitialDelay);
        assertEquals(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT, entityUserOperator.livenessTimeout);
        assertEquals(uoWatchedNamespace, entityUserOperator.getWatchedNamespace());
        assertEquals(uoReconciliationInterval * 1000, entityUserOperator.getReconciliationIntervalMs());
        assertEquals(uoZookeeperSessionTimeout * 1000, entityUserOperator.getZookeeperSessionTimeoutMs());
        assertEquals(EntityUserOperator.defaultZookeeperConnect(cluster), entityUserOperator.getZookeeperConnect());
        assertEquals(userOperatorLogging.getType(), entityUserOperator.getLogging().getType());
        assertEquals(userOperatorLogging.getLoggers(), ((InlineLogging) entityUserOperator.getLogging()).getLoggers());
    }

    @Test
    public void testFromCrdDefault() {
        EntityUserOperatorSpec entityUserOperatorSpec = new EntityUserOperatorSpecBuilder()
                .build();
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withUserOperator(entityUserOperatorSpec)
                .build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(resource);

        assertEquals(namespace, entityUserOperator.getWatchedNamespace());
        assertEquals("strimzi/operator:latest", entityUserOperator.getImage());
        assertEquals(EntityUserOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1000, entityUserOperator.getReconciliationIntervalMs());
        assertEquals(EntityUserOperatorSpec.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1000, entityUserOperator.getZookeeperSessionTimeoutMs());
        assertEquals(EntityUserOperator.defaultZookeeperConnect(cluster), entityUserOperator.getZookeeperConnect());
        assertNull(entityUserOperator.getLogging());
    }

    @Test
    public void testFromCrdNoEntityOperator() {
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout);
        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(resource);
        assertNull(entityUserOperator);
    }

    @Test
    public void testFromCrdNoUserOperatorInEntityOperator() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder().build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(resource);
        assertNull(entityUserOperator);
    }

    @Test
    public void testGetContainers() {
        List<Container> containers = entityUserOperator.getContainers(null);
        assertEquals(1, containers.size());

        Container container = containers.get(0);
        assertEquals(EntityUserOperator.USER_OPERATOR_CONTAINER_NAME, container.getName());
        assertEquals(entityUserOperator.getImage(), container.getImage());
        assertEquals(getExpectedEnvVars(), container.getEnv());
        assertEquals(new Integer(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_DELAY), container.getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT), container.getLivenessProbe().getTimeoutSeconds());
        assertEquals(new Integer(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_DELAY), container.getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT), container.getReadinessProbe().getTimeoutSeconds());
        assertEquals(1, container.getPorts().size());
        assertEquals(new Integer(EntityUserOperator.HEALTHCHECK_PORT), container.getPorts().get(0).getContainerPort());
        assertEquals(EntityUserOperator.HEALTHCHECK_PORT_NAME, container.getPorts().get(0).getName());
        assertEquals("TCP", container.getPorts().get(0).getProtocol());
        assertEquals("/opt/user-operator/custom-config/", container.getVolumeMounts().get(0).getMountPath());
        assertEquals("entity-user-operator-metrics-and-logging", container.getVolumeMounts().get(0).getName());
    }

    @Test
    public void testFromCrdCaValidityAndRenewal() {
        EntityUserOperatorSpec entityUserOperatorSpec = new EntityUserOperatorSpecBuilder()
                .build();
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withUserOperator(entityUserOperatorSpec)
                .build();
        CertificateAuthority ca = new CertificateAuthority();
        ca.setValidityDays(42);
        ca.setRenewalDays(69);
        Kafka customValues =
                new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .withClientsCa(ca)
                        .endSpec()
                        .build();
        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(customValues);

        Kafka defaultValues =
                new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityUserOperator entityUserOperator2 = EntityUserOperator.fromCrd(defaultValues);

        assertEquals(42, entityUserOperator.getClientsCaValidityDays());
        assertEquals(69, entityUserOperator.getClientsCaRenewalDays());
        assertEquals(CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS, entityUserOperator2.getClientsCaValidityDays());
        assertEquals(CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS, entityUserOperator2.getClientsCaRenewalDays());
    }

    @Test
    public void testEntityUserOperatorEnvVarValidityAndRenewal() {
        int validity = 100;
        int renewal = 42;
        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, singletonMap("animal", "wombat"), singletonMap("foo", "bar"), emptyMap()))
                .editSpec()
                .withNewClientsCa()
                .withRenewalDays(renewal)
                .withValidityDays(validity)
                .endClientsCa()
                .withNewEntityOperator()
                .withNewUserOperator()
                .endUserOperator()
                .endEntityOperator()
                .endSpec()
                .build();

        EntityUserOperator f = EntityUserOperator.fromCrd(kafkaAssembly);
        List<EnvVar> envvar = f.getEnvVars();
        assertEquals(validity, Integer.parseInt(envvar.stream().filter(a -> a.getName().equals(EntityUserOperator.ENV_VAR_CLIENTS_CA_VALIDITY)).findFirst().get().getValue()));
        assertEquals(renewal, Integer.parseInt(envvar.stream().filter(a -> a.getName().equals(EntityUserOperator.ENV_VAR_CLIENTS_CA_RENEWAL)).findFirst().get().getValue()));
    }

    @Test
    public void testRoleBinding()   {
        KubernetesRoleBinding binding = entityUserOperator.generateRoleBinding(namespace, uoWatchedNamespace);

        assertEquals(namespace, binding.getSubjects().get(0).getNamespace());
        assertEquals(uoWatchedNamespace, binding.getMetadata().getNamespace());
    }
}
