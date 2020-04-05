/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.zk;

import io.strimzi.test.EmbeddedZooKeeper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class ZkImplTest {

    private EmbeddedZooKeeper zkServer;

    private static Vertx vertx;
    private Zk zk;

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
    }

    @AfterEach
    public void teardown(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        Promise<Void> zkDisconnected = Promise.promise();
        zk.disconnect(result -> zkDisconnected.complete());

        zkDisconnected.future().compose(v -> {
            if (this.zkServer != null) {
                this.zkServer.close();
            }
            async.flag();
            return Future.succeededFuture();
        });

    }

    @Disabled
    @Test
    public void testReconnectOnBounce(VertxTestContext context) throws IOException, InterruptedException {
        Checkpoint async = context.checkpoint();

        Zk zkImpl = Zk.createSync(vertx, zkServer.getZkConnectString(), 60_000, 10_000);
        zkServer.restart();

        Promise fooCreated = Promise.promise();

        zkImpl.create("/foo", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, context.succeeding(v -> fooCreated.complete()));

        fooCreated.future().compose(v -> {
            try {
                zkServer.restart();
                // TODO Without the sleep this test fails, because there's a race between the creation of /bar
                // and the reconnection within ZkImpl. We probably need to fix ZkImpl to retry if things fail due to
                // connection loss, possibly with some limit on the number of retries.
                // TODO We also need to reset the watches on reconnection.
                Thread.sleep(2000);
                zkImpl.create("/bar", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, context.succeeding(vv -> async.flag()));
            } catch (Exception e) {
                context.failNow(e);
            }
            return Future.succeededFuture();
        });

    }

    @Test
    public void testWatchThenUnwatchChildren(VertxTestContext context) {
        Checkpoint async = context.checkpoint(2);

        Promise fooCreated = Promise.promise();

        // Create a node
        zk.create("/foo", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, context.succeeding(v -> fooCreated.complete()));

        fooCreated.future().compose(v -> {
            // Now watch its children
            zk.watchChildren("/foo", context.succeeding(watchResult -> {
                if (watchResult.equals(singletonList("bar"))) {
                    zk.unwatchChildren("/foo");
                    zk.delete("/foo/bar", -1, deleteResult -> async.flag());
                }
            }))
                .compose(ignored -> {
                    zk.children("/foo",  context.succeeding(lsResult -> {
                        context.verify(() -> assertThat(lsResult, is(emptyList())));
                        zk.create("/foo/bar", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ig -> async.flag());
                    }));
                    return Future.succeededFuture();
                });
            return Future.succeededFuture();
        });
    }

    @Test
    public void testWatchThenUnwatchData(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        Promise fooCreated = Promise.promise();

        // Create a node
        byte[] data1 = new byte[]{1};
        zk.create("/foo", data1, AclBuilder.PUBLIC, CreateMode.PERSISTENT, context.succeeding(v -> fooCreated.complete()));

        fooCreated.future().compose(v -> {
            byte[] data2 = {2};
            return zk.watchData("/foo", context.succeeding(dataWatch -> {
                context.verify(() -> assertThat(dataWatch, is(data2)));
            }))
            .compose(zk -> {
                zk.getData("/foo", context.succeeding(dataResult -> {
                    context.verify(() -> assertThat(dataResult, is(data1)));

                    zk.setData("/foo", data2, -1, context.succeeding(setResult -> async.flag()));
                }));
                return Future.succeededFuture();
            });
        });
    }
}
