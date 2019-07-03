/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.operator.resource.ClusterRoleOperator;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

@RunWith(VertxUnitRunner.class)
public class MainIT {
    private Vertx vertx;
    private KubernetesClient client;

    @Before
    public void createClient(TestContext context) {
        vertx = Vertx.vertx();
        client = new DefaultKubernetesClient();
    }

    @After
    public void closeClient() {
        vertx.close();
        client.close();
    }

    @Test
    public void testCreateClusterRoles(TestContext context) {
        Map<String, String> envVars = new HashMap<>(1);
        envVars.put(ClusterOperatorConfig.STRIMZI_CREATE_CLUSTER_ROLES, "TRUE");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_S2I_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo");
        envVars.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo");

        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(envVars);

        ClusterRoleOperator cro = new ClusterRoleOperator(vertx, client);

        Async async = context.async();
        Main.maybeCreateClusterRoles(vertx, config, client).setHandler(res -> {
            context.assertTrue(res.succeeded());

            context.assertNotNull(cro.get("strimzi-cluster-operator-namespaced"));
            context.assertNotNull(cro.get("strimzi-cluster-operator-global"));
            context.assertNotNull(cro.get("strimzi-kafka-broker"));
            context.assertNotNull(cro.get("strimzi-entity-operator"));
            context.assertNotNull(cro.get("strimzi-topic-operator"));

            async.complete();
        });

        async.awaitSuccess();
    }
}
