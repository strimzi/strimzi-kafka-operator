/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.operator.resource.ClusterRoleOperator;
import io.strimzi.test.k8s.cluster.KubeCluster;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@ExtendWith(VertxExtension.class)
public class MainIT {
    private static Vertx vertx;
    private KubernetesClient client;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @BeforeEach
    private void createClient() {
        client = new DefaultKubernetesClient();
    }

    @AfterEach
    private void closeClient() {
        client.close();
    }

    @Test
    public void testCreateClusterRolesCreatesClusterRoles(VertxTestContext context) {
        assertDoesNotThrow(() -> KubeCluster.bootstrap());
        Map<String, String> envVars = new HashMap<>(6);
        envVars.put(ClusterOperatorConfig.STRIMZI_CREATE_CLUSTER_ROLES, "TRUE");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_S2I_IMAGES, KafkaVersionTestUtils.getKafkaConnectS2iImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMakerImagesEnvVarString());
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString());

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars, KafkaVersionTestUtils.getKafkaVersionLookup());

        ClusterRoleOperator cro = new ClusterRoleOperator(vertx, client, 100);

        Checkpoint a = context.checkpoint();
        Main.maybeCreateClusterRoles(vertx, config, client)
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat(cro.get("strimzi-cluster-operator-namespaced"), is(notNullValue()));
                assertThat(cro.get("strimzi-cluster-operator-global"), is(notNullValue()));
                assertThat(cro.get("strimzi-kafka-broker"), is(notNullValue()));
                assertThat(cro.get("strimzi-entity-operator"), is(notNullValue()));
                assertThat(cro.get("strimzi-topic-operator"), is(notNullValue()));
                a.flag();
            })));
    }
}
