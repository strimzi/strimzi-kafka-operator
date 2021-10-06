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
//        KafkaRoller kafkaRoller = new KafkaRoller();
//        kafkaRoller.rollingRestart()
    }

    // TODO Case: ordering of queue with different context states
    // TODO: Scheduling of tasks
    // TODO: Parallelism
    // TODO: cluster-level invariants aren't violated
    // TODO: threading
    // TODO: global behaviour around retrying and bounding the total time.

}
