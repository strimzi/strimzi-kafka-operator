/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

import static java.util.Collections.singletonMap;

@RunWith(VertxUnitRunner.class)
public class ClusterControllerTest {

    private static Vertx vertx;

    @BeforeClass
    public static void beforeClass() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void afterClass() {
        vertx.close();
    }

    @Test
    @Ignore
    public void test0() {
        String namespace = "ns";
        Map labels = singletonMap("strimzi.io/kind", "cluster");
        ClusterControllerConfig config = new ClusterControllerConfig(namespace, labels);
        ClusterController cc = new ClusterController(config);
        vertx.deployVerticle(cc);

        //cc.
    }
}
