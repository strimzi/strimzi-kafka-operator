/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.EntityUserOperatorSpecBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.operator.MockCertManager;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class EntityOperatorTest {

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;

    private final EntityUserOperatorSpec entityUserOperatorSpec = new EntityUserOperatorSpecBuilder()
            .build();
    private final EntityTopicOperatorSpec entityTopicOperatorSpec = new EntityTopicOperatorSpecBuilder()
            .build();

    private final EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
            .withTopicOperator(entityTopicOperatorSpec)
            .withUserOperator(entityUserOperatorSpec)
            .build();

    private final Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                    .endSpec()
                    .build();

    private final CertManager certManager = new MockCertManager();
    private final EntityOperator entityOperator = EntityOperator.fromCrd(certManager, resource, ResourceUtils.createKafkaClusterInitialSecrets(namespace, cluster));

    @Test
    public void testGenerateDeployment() {

        Deployment dep = entityOperator.generateDeployment();

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertEquals(entityOperator.entityOperatorName(cluster), dep.getMetadata().getName());
        assertEquals(namespace, dep.getMetadata().getNamespace());
        assertEquals(new Integer(EntityOperatorSpec.DEFAULT_REPLICAS), dep.getSpec().getReplicas());

        assertEquals(3, containers.size());
        // just check names of topic and user operators (their containers are tested in the related unit test classes)
        assertEquals(EntityTopicOperator.TOPIC_OPERATOR_NAME, containers.get(0).getName());
        assertEquals(EntityUserOperator.USER_OPERATOR_NAME, containers.get(1).getName());
        // checks on the TLS sidecar container
        assertEquals(EntityOperatorSpec.DEFAULT_TLS_SIDECAR_IMAGE, containers.get(2).getImage());
        assertEquals(EntityOperator.defaultZookeeperConnect(cluster), AbstractModel.containerEnvVars(containers.get(2)).get(EntityOperator.ENV_VAR_ZOOKEEPER_CONNECT));
        assertEquals(EntityOperator.TLS_SIDECAR_VOLUME_NAME, containers.get(2).getVolumeMounts().get(0).getName());
        assertEquals(EntityOperator.TLS_SIDECAR_VOLUME_MOUNT, containers.get(2).getVolumeMounts().get(0).getMountPath());
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
        EntityOperator entityOperator = EntityOperator.fromCrd(certManager, resource, ResourceUtils.createKafkaClusterInitialSecrets(namespace, cluster));

        assertNull(entityOperator.getTopicOperator());
        assertNull(entityOperator.getUserOperator());
    }

    @Rule
    public ResourceTester<Kafka, EntityOperator> helper = new ResourceTester<>(Kafka.class, EntityOperator::fromCrd);

    @Test
    public void withAffinity() throws IOException {
        helper.assertDesiredResource("-Deployment.yaml", zc -> zc.generateDeployment().getSpec().getTemplate().getSpec().getAffinity());
    }

    @AfterClass
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}
