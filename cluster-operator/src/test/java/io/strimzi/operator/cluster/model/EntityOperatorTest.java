/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.EntityUserOperatorSpecBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarBuilder;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.test.TestUtils;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.test.TestUtils.map;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class EntityOperatorTest {

    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(new StringReader(
            "2.0.0 default 2.0 2.0 1234567890abcdef\n" +
            "2.1.0         2.1 2.0 1234567890abcdef"),
            map("2.0.0", "strimzi/kafka:latest-kafka-2.0.0",
                    "2.1.0", "strimzi/kafka:latest-kafka-2.1.0"), emptyMap(), emptyMap(), emptyMap()) { };

    static Map<String, String> volumeMounts(List<VolumeMount> mounts) {
        return mounts.stream().collect(Collectors.toMap(vm -> vm.getName(), vm -> vm.getMountPath()));
    }

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final int tlsHealthDelay = 120;
    private final int tlsHealthTimeout = 30;

    private final EntityUserOperatorSpec entityUserOperatorSpec = new EntityUserOperatorSpecBuilder()
            .build();
    private final EntityTopicOperatorSpec entityTopicOperatorSpec = new EntityTopicOperatorSpecBuilder()
            .build();
    private final TlsSidecar tlsSidecar = new TlsSidecarBuilder()
            .withLivenessProbe(new ProbeBuilder().withInitialDelaySeconds(tlsHealthDelay).withTimeoutSeconds(tlsHealthTimeout).build())
            .withReadinessProbe(new ProbeBuilder().withInitialDelaySeconds(tlsHealthDelay).withTimeoutSeconds(tlsHealthTimeout).build())
            .build();

    private final EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
            .withTlsSidecar(tlsSidecar)
            .withTopicOperator(entityTopicOperatorSpec)
            .withUserOperator(entityUserOperatorSpec)
            .build();

    private final Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                    .endSpec()
                    .build();

    private final EntityOperator entityOperator = EntityOperator.fromCrd(resource, VERSIONS);

    @Test
    public void testGenerateDeployment() {

        Deployment dep = entityOperator.generateDeployment(true, Collections.EMPTY_MAP, null, null);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(entityOperator.entityOperatorName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        assertEquals(new Integer(EntityOperatorSpec.DEFAULT_REPLICAS), dep.getSpec().getReplicas());
        assertEquals(1, dep.getMetadata().getOwnerReferences().size());
        assertEquals(entityOperator.createOwnerReference(), dep.getMetadata().getOwnerReferences().get(0));

        assertEquals(3, containers.size());
        // just check names of topic and user operators (their containers are tested in the related unit test classes)
        assertEquals(EntityTopicOperator.TOPIC_OPERATOR_CONTAINER_NAME, containers.get(0).getName());
        assertEquals(EntityUserOperator.USER_OPERATOR_CONTAINER_NAME, containers.get(1).getName());
        // checks on the TLS sidecar container
        Container tlsSidecarContainer = containers.get(2);
        assertEquals(image, tlsSidecarContainer.getImage());
        assertEquals(EntityOperator.defaultZookeeperConnect(cluster), AbstractModel.containerEnvVars(tlsSidecarContainer).get(EntityOperator.ENV_VAR_ZOOKEEPER_CONNECT));
        assertEquals(TlsSidecarLogLevel.NOTICE.toValue(), AbstractModel.containerEnvVars(tlsSidecarContainer).get(ModelUtils.TLS_SIDECAR_LOG_LEVEL));
        assertEquals(map(
                EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT,
                EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT),
                EntityOperatorTest.volumeMounts(tlsSidecarContainer.getVolumeMounts()));
        assertEquals(new Integer(tlsHealthDelay), tlsSidecarContainer.getReadinessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(tlsHealthTimeout), tlsSidecarContainer.getReadinessProbe().getTimeoutSeconds());
        assertEquals(new Integer(tlsHealthDelay), tlsSidecarContainer.getLivenessProbe().getInitialDelaySeconds());
        assertEquals(new Integer(tlsHealthTimeout), tlsSidecarContainer.getLivenessProbe().getTimeoutSeconds());
    }

    @Test
    public void testFromCrd() {
        assertEquals(namespace, entityOperator.namespace);
        assertEquals(cluster, entityOperator.cluster);
        assertEquals(EntityOperator.defaultZookeeperConnect(cluster), entityOperator.getZookeeperConnect());
    }

    @Test
    public void testFromCrdNoTopicAndUserOperatorInEntityOperator() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder().build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityOperator entityOperator = EntityOperator.fromCrd(resource, VERSIONS);

        assertNull(entityOperator.getTopicOperator());
        assertNull(entityOperator.getUserOperator());
    }

    @Rule
    public ResourceTester<Kafka, EntityOperator> helper = new ResourceTester<>(Kafka.class, VERSIONS, EntityOperator::fromCrd);

    @Test
    public void withOldAffinity() throws IOException {
        helper.assertDesiredResource("-Deployment.yaml", zc -> zc.generateDeployment(true, Collections.EMPTY_MAP, null, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    @Test
    public void withAffinity() throws IOException {
        helper.assertDesiredResource("-Deployment.yaml", zc -> zc.generateDeployment(true, Collections.EMPTY_MAP, null, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    @Test
    public void testTemplate() {
        Map<String, String> depLabels = TestUtils.map("l1", "v1", "l2", "v2");
        Map<String, String> depAnots = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = TestUtils.map("a3", "v3", "a4", "v4");

        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                            .withNewEntityOperator()
                                .withTopicOperator(entityTopicOperatorSpec)
                                .withUserOperator(entityUserOperatorSpec)
                                .withNewTemplate()
                                    .withNewDeployment()
                                        .withNewMetadata()
                                            .withLabels(depLabels)
                                            .withAnnotations(depAnots)
                                        .endMetadata()
                                    .endDeployment()
                                    .withNewPod()
                                        .withNewMetadata()
                                            .withLabels(podLabels)
                                            .withAnnotations(podAnots)
                                        .endMetadata()
                                    .endPod()
                                .endTemplate()
                            .endEntityOperator()
                        .endSpec()
                        .build();
        EntityOperator entityOperator = EntityOperator.fromCrd(resource, VERSIONS);

        // Check Deployment
        Deployment dep = entityOperator.generateDeployment(true, Collections.EMPTY_MAP, null, null);
        assertTrue(dep.getMetadata().getLabels().entrySet().containsAll(depLabels.entrySet()));
        assertTrue(dep.getMetadata().getAnnotations().entrySet().containsAll(depAnots.entrySet()));

        // Check Pods
        assertTrue(dep.getSpec().getTemplate().getMetadata().getLabels().entrySet().containsAll(podLabels.entrySet()));
        assertTrue(dep.getSpec().getTemplate().getMetadata().getAnnotations().entrySet().containsAll(podAnots.entrySet()));
    }

    @Test
    public void testGracePeriod() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                    .withTopicOperator(entityTopicOperatorSpec)
                    .withUserOperator(entityUserOperatorSpec)
                    .withNewTemplate()
                        .withNewPod()
                            .withTerminationGracePeriodSeconds(123)
                        .endPod()
                    .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, null);
        assertEquals(Long.valueOf(123), dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds());
        assertNotNull(dep.getSpec().getTemplate().getSpec().getContainers().get(2).getLifecycle());
        assertTrue(dep.getSpec().getTemplate().getSpec().getContainers().get(2).getLifecycle().getPreStop().getExec().getCommand().contains("/opt/stunnel/entity_operator_stunnel_pre_stop.sh"));
        assertTrue(dep.getSpec().getTemplate().getSpec().getContainers().get(2).getLifecycle().getPreStop().getExec().getCommand().contains("123"));
    }

    @Test
    public void testDefaultGracePeriod() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, null);
        assertEquals(Long.valueOf(30), dep.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds());
        assertNotNull(dep.getSpec().getTemplate().getSpec().getContainers().get(2).getLifecycle());
        assertTrue(dep.getSpec().getTemplate().getSpec().getContainers().get(2).getLifecycle().getPreStop().getExec().getCommand().contains("/opt/stunnel/entity_operator_stunnel_pre_stop.sh"));
        assertTrue(dep.getSpec().getTemplate().getSpec().getContainers().get(2).getLifecycle().getPreStop().getExec().getCommand().contains("30"));
    }

    @Test
    public void testImagePullSecrets() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                    .withTopicOperator(entityTopicOperatorSpec)
                    .withUserOperator(entityUserOperatorSpec)
                    .withNewTemplate()
                        .withNewPod()
                            .withImagePullSecrets(secret1, secret2)
                        .endPod()
                    .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, null);
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1));
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2));
    }

    @Test
    public void testImagePullSecretsFromCo() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        List<LocalObjectReference> secrets = new ArrayList<>(2);
        secrets.add(secret1);
        secrets.add(secret2);

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, secrets);
        assertEquals(2, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1));
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2));
    }

    @Test
    public void testImagePullSecretsFromBoth() {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        Kafka resource = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                .withNewEntityOperator()
                .withTopicOperator(entityTopicOperatorSpec)
                .withUserOperator(entityUserOperatorSpec)
                .withNewTemplate()
                .withNewPod()
                .withImagePullSecrets(secret2)
                .endPod()
                .endTemplate()
                .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, singletonList(secret1));
        assertEquals(1, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
        assertFalse(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret1));
        assertTrue(dep.getSpec().getTemplate().getSpec().getImagePullSecrets().contains(secret2));
    }

    @Test
    public void testDefaultImagePullSecrets() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, null);
        assertEquals(0, dep.getSpec().getTemplate().getSpec().getImagePullSecrets().size());
    }

    @Test
    public void testSecurityContext() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                        .withNewTemplate()
                            .withNewPod()
                                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                            .endPod()
                        .endTemplate()
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, null);
        assertNotNull(dep.getSpec().getTemplate().getSpec().getSecurityContext());
        assertEquals(Long.valueOf(123), dep.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup());
        assertEquals(Long.valueOf(456), dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsGroup());
        assertEquals(Long.valueOf(789), dep.getSpec().getTemplate().getSpec().getSecurityContext().getRunAsUser());
    }

    @Test
    public void testDefaultSecurityContext() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, null, null);
        assertNull(dep.getSpec().getTemplate().getSpec().getSecurityContext());
    }

    /**
     * Verify the lookup order is:<ul>
     * <li>Kafka.spec.entityOperator.tlsSidecar.image</li>
     * <li>Kafka.spec.kafka.image</li>
     * <li>image for default version of Kafka</li></ul>
     */
    @Test
    public void testStunnelImage() {
        Kafka kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editEntityOperator()
                        .editOrNewTlsSidecar()
                            .withImage("foo1")
                        .endTlsSidecar()
                    .endEntityOperator()
                    .editKafka()
                        .withImage("foo2")
                    .endKafka()
                .endSpec()
                .build();
        assertEquals("foo1", EntityOperator.fromCrd(kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(2).getImage());

        kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editEntityOperator()
                        .editOrNewTlsSidecar()
                            .withImage(null)
                        .endTlsSidecar()
                    .endEntityOperator()
                    .editKafka()
                        .withImage("foo2")
                    .endKafka()
                .endSpec()
                .build();
        assertEquals("foo2", EntityOperator.fromCrd(kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(2).getImage());

        kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editEntityOperator()
                        .editOrNewTlsSidecar()
                            .withImage(null)
                        .endTlsSidecar()
                    .endEntityOperator()
                    .editKafka()
                        .withVersion("2.0.0")
                        .withImage(null)
                    .endKafka()
                .endSpec()
            .build();
        assertEquals("strimzi/kafka:latest-kafka-2.0.0", EntityOperator.fromCrd(kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(2).getImage());

        kafka = new KafkaBuilder(resource)
                .editSpec()
                    .editEntityOperator()
                        .editOrNewTlsSidecar()
                            .withImage(null)
                        .endTlsSidecar()
                    .endEntityOperator()
                    .editKafka()
                        .withVersion("2.1.0")
                        .withImage(null)
                    .endKafka()
                .endSpec()
            .build();
        assertEquals("strimzi/kafka:latest-kafka-2.0.0", EntityOperator.fromCrd(kafka, VERSIONS).getContainers(ImagePullPolicy.ALWAYS).get(2).getImage());
    }

    @Test
    public void testImagePullPolicy() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withNewEntityOperator()
                        .withTopicOperator(entityTopicOperatorSpec)
                        .withUserOperator(entityUserOperatorSpec)
                    .endEntityOperator()
                .endSpec()
                .build();
        EntityOperator eo = EntityOperator.fromCrd(resource, VERSIONS);

        Deployment dep = eo.generateDeployment(true, Collections.EMPTY_MAP, ImagePullPolicy.ALWAYS, null);
        assertEquals(ImagePullPolicy.ALWAYS.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());
        assertEquals(ImagePullPolicy.ALWAYS.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy());
        assertEquals(ImagePullPolicy.ALWAYS.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(2).getImagePullPolicy());

        dep = eo.generateDeployment(true, Collections.EMPTY_MAP, ImagePullPolicy.IFNOTPRESENT, null);
        assertEquals(ImagePullPolicy.IFNOTPRESENT.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy());
        assertEquals(ImagePullPolicy.IFNOTPRESENT.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy());
        assertEquals(ImagePullPolicy.IFNOTPRESENT.toString(), dep.getSpec().getTemplate().getSpec().getContainers().get(2).getImagePullPolicy());
    }

    @AfterClass
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}
