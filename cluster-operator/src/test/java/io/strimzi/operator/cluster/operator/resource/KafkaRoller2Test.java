/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.List;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;

public class KafkaRoller2Test {


    private static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    public void test1() {
        ClusterModel clusterModel = new ClusterModel()
                .addNBrokers(3)
                .addNewTopic("my-topic")
                    .addNewPartition(0)
                        .leader(0)
                        .replicaOn(0, 1, 2)
                        .isr(0, 1, 2)
                    .endPartition()
                .endTopic();
        Admin admin = clusterModel.buildAdminClient();
        PodOperator podOps = mock(PodOperator.class);
        //when(podOps.getAsync(eq(""), eq(""))).thenReturn();
        var roller = new KafkaRoller(Reconciliation.DUMMY_RECONCILIATION,
                vertx,
                podOps,
                1,
                10,
                null,
                null,
                null,
                null,
                (w, x, y, z) -> admin,
                null,
                null,
                null,
                false);
        roller.rollingRestart(pod -> List.of("test"));
        // Assertions on:
        // Which pods got rolled
        // The order the pods got rolled
        // Any exceptional outcode from the rolling
        // That fresh info was used to determine rollability
    }

    // Exception getting AC (from supplier)
    // Exception closing AC
    // Timeouts from every method of the AC
    // Expected errors from every method of the AC
    // The number of requests before a pod is rollable
    // Timeouts and errors from K8s
    //
}
