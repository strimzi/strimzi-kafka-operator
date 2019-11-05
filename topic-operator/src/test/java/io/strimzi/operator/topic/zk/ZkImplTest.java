/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.zk;

import io.strimzi.test.EmbeddedZooKeeper;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class ZkImplTest {

    private EmbeddedZooKeeper zkServer;

    private Vertx vertx = Vertx.vertx();
    private Zk zk;

    @BeforeEach
    public void setup()
            throws IOException, InterruptedException {
        this.zkServer = new EmbeddedZooKeeper();
        zk = Zk.createSync(vertx, zkServer.getZkConnectString(), 60_000, 10_000);
    }

    @AfterEach
    public void teardown(VertxTestContext context) {
        Checkpoint async = context.checkpoint(2);
        zk.disconnect(result -> async.flag());
        async.flag();
        if (this.zkServer != null) {
            this.zkServer.close();
        }
        vertx.close();
    }

    @Disabled
    @Test
    public void testReconnectOnBounce(VertxTestContext context) throws IOException, InterruptedException {
        Zk zkImpl = Zk.createSync(vertx, zkServer.getZkConnectString(), 60_000, 10_000);
        zkServer.restart();
        Checkpoint async = context.checkpoint(2);
        zkImpl.create("/foo", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ar -> {
            context.verify(() -> assertThat(ar.succeeded(), is(true)));
            async.flag();
        });
        async.flag();
        zkServer.restart();
        // TODO Without the sleep this test fails, because there's a race between the creation of /bar
        // and the reconnection within ZkImpl. We probably need to fix ZkImpl to retry if things fail due to
        // connection loss, possibly with some limit on the number of retries.
        // TODO We also need to reset the watches on reconnection.
        Thread.sleep(2000);
        Checkpoint async2 = context.checkpoint();
        zkImpl.create("/bar", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ar -> {
            //ar.cause().printStackTrace();
            context.verify(() -> assertThat(ar.toString(), ar.succeeded(), is(true)));
            async2.flag();
        });
    }

    @Test
    public void testWatchUnwatchChildren(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        // Create a node
        CompletableFuture<Boolean> fooFuture = new CompletableFuture<>();
        zk.create("/foo", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ar -> {
            fooFuture.complete(true);
        });
        fooFuture.get(60, TimeUnit.SECONDS);

        // Now watch its children
        CompletableFuture<Boolean> barFuture = new CompletableFuture<>();
        zk.watchChildren("/foo", watchResult -> {
            if (singletonList("bar").equals(watchResult.result())) {
                zk.unwatchChildren("/foo");
                zk.delete("/foo/bar", -1, deleteResult -> {
                    barFuture.complete(true);
                });
            }
        }).<Void>compose(ignored -> {
            zk.children("/foo", lsResult -> {
                context.verify(() -> assertThat(lsResult.result(), is(emptyList())));
                zk.create("/foo/bar", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ig -> { });
            });
            return Future.succeededFuture();
        });
        barFuture.get(60, TimeUnit.SECONDS);
        context.completeNow();
    }

    @Test
    public void testWatchUnwatchData(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        // Create a node
        CompletableFuture<Boolean> fooFuture = new CompletableFuture<>();
        byte[] data1 = new byte[]{1};
        zk.create("/foo", data1, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ar -> {
            fooFuture.complete(true);
        });
        fooFuture.get(60, TimeUnit.SECONDS);

        Checkpoint done = context.checkpoint();
        byte[] data2 = {2};
        zk.watchData("/foo", dataWatch -> {
            context.verify(() -> assertThat(dataWatch.result(), is(data2)));
        }).compose(zk2 -> {
            zk.getData("/foo", dataResult -> {
                context.verify(() -> assertThat(dataResult.result(), is(data1)));

                zk.setData("/foo", data2, -1, setResult -> {
                    done.flag();
                });
            });
            return Future.succeededFuture();
        });
    }

}
