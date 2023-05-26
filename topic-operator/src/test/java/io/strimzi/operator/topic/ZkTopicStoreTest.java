/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.zk.Zk;
import io.strimzi.test.EmbeddedZooKeeper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public class ZkTopicStoreTest extends TopicStoreTestBase {
    private static Vertx vertx;
    private EmbeddedZooKeeper zkServer;
    private Zk zkClient;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @BeforeEach
    public void setup(VertxTestContext context) throws Exception {
        zkServer = new EmbeddedZooKeeper();
        zkClient = Zk.createSync(vertx, zkServer.getZkConnectString(), 60_000, 10_000);
        String topicsPath = "/strimzi/topics";
        this.store = new ZkTopicStore(zkClient, topicsPath);
        // wait for topic store initialization before moving ahead with test execution
        zkClient.watchChildren(topicsPath, context.succeeding(watchResult -> {
            zkClient.unwatchChildren(topicsPath);
            context.completeNow();
        }));
    }

    @AfterEach
    public void teardown(VertxTestContext context) {
        Checkpoint zkDisconnected = context.checkpoint();

        Promise<Void> promise = Promise.promise();
        zkClient.disconnect(result -> promise.complete());

        promise.future().compose(v -> {
            if (this.zkServer != null) {
                this.zkServer.close();
            }
            zkDisconnected.flag();
            return Future.succeededFuture();
        });
    }
}
