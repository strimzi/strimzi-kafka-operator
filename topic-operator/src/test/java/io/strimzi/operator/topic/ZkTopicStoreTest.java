/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.zk.Zk;
import io.strimzi.test.EmbeddedZooKeeper;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

public class ZkTopicStoreTest extends TopicStoreTestBase {

    private static Vertx vertx;

    private EmbeddedZooKeeper zkServer;
    private Zk zk;

    @Override
    protected boolean canRunTest() {
        return false; // test was disabled
    }

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @BeforeEach
    public void setup() throws IOException, InterruptedException {
        this.zkServer = new EmbeddedZooKeeper();
        zk = Zk.createSync(vertx, zkServer.getZkConnectString(), 60_000, 10_000);
        this.store = new ZkTopicStore(zk, "/strimzi/topics");
    }

    @AfterEach
    public void teardown(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        zk.disconnect(ar -> async.flag());
        if (this.zkServer != null) {
            this.zkServer.close();
        }
    }
}
