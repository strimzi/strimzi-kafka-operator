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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class ZkImplTest {

    private EmbeddedZooKeeper zkServer;

    private Zk zk;

    @BeforeEach
    public void setup(Vertx vertx) throws IOException, InterruptedException {
        this.zkServer = new EmbeddedZooKeeper();
        zk = Zk.createSync(vertx, zkServer.getZkConnectString(), 60_000, 10_000);
    }

    //For fun and games add a VertxTestContext as an injected parameter, and watch Vert.x's Junit code hang the test.
    @AfterEach
    public void teardown() throws ExecutionException, InterruptedException {
        Promise<Void> zkDisconnected = Promise.promise();
        zk.disconnect(result -> zkDisconnected.complete());
        zkDisconnected.future().toCompletionStage().toCompletableFuture().get();
    }

    @Test
    public void testReconnectOnBounce(Vertx vertx, VertxTestContext context) throws IOException, InterruptedException {

        Zk zkImpl = Zk.createSync(vertx, zkServer.getZkConnectString(), 60_000, 10_000);
        zkServer.restart();

        Promise<Void> fooCreated = Promise.promise();

        zkImpl.create("/foo", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, context.succeeding(v -> fooCreated.complete()));

        fooCreated.future().compose(v -> {
            try {
                zkServer.restart();
                zkImpl.create("/bar", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, x-> {});
            } catch (Exception e) {
                context.failNow(e);
            }
            return Future.succeededFuture();
        }).onComplete(x -> context.completeNow());

    }

    @Test
    public void testWatchThenUnwatchChildren(VertxTestContext context) {

        //Left these checkpoints intact because they seem to be relevant to use both.
        Checkpoint deletionOccurred = context.checkpoint();
        Checkpoint createOccurred = context.checkpoint();


        Promise<Void> fooCreated = Promise.promise();

        // Create a node
        zk.create("/foo", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, context.succeeding(v -> fooCreated.complete()));

        fooCreated.future().compose(v -> {
            // Now watch its children
            zk.watchChildren("/foo", context.succeeding(watchResult -> {
                if (watchResult.equals(singletonList("bar"))) {
                    zk.unwatchChildren("/foo");
                    zk.delete("/foo/bar", -1, deleteResult -> deletionOccurred.flag());
                }
            }))
                .compose(ignored -> {
                    zk.children("/foo",  context.succeeding(lsResult -> {
                        context.verify(() -> assertThat(lsResult, is(emptyList())));
                        zk.create("/foo/bar", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ig -> {
                            //Nota bene - as this is the last checkpoint, flagging it completes the test
                            createOccurred.flag();
                        });
                    }));
                    return Future.succeededFuture();
                });
            return Future.succeededFuture();
        });
    }

    @Test
    public void testWatchThenUnwatchData(VertxTestContext context) {
        Promise<Void> fooCreated = Promise.promise();

        // Create a node
        byte[] data1 = new byte[]{1};
        zk.create("/foo", data1, AclBuilder.PUBLIC, CreateMode.PERSISTENT, context.succeeding(v -> fooCreated.complete()));

        fooCreated.future().compose(v -> {
            byte[] data2 = {2};
            return zk.watchData("/foo", context.succeeding(dataWatch -> context.verify(() -> assertThat(dataWatch, is(data2)))))
            .compose(zk -> {
                zk.getData("/foo", context.succeeding(dataResult -> {
                    context.verify(() -> assertThat(dataResult, is(data1)));

                    zk.setData("/foo", data2, -1, context.succeeding(setResult -> context.completeNow()));
                }));
                return Future.succeededFuture();
            });
        });
    }

    @Test
    public void testPathExists(VertxTestContext context) {

        Promise<Void> fooCreated = Promise.promise();

        // Create a node
        byte[] data1 = new byte[]{1};
        zk.create("/foo", data1, AclBuilder.PUBLIC, CreateMode.PERSISTENT, context.succeeding(v -> fooCreated.complete()));

        fooCreated
            .future()
            .compose(v -> zk.pathExists("/foo"))
            .onComplete(context.succeeding(b -> context.verify(() -> assertThat("Zk path doesn't exist", b))))
            .onComplete(context.succeeding(v -> context.completeNow()));
    }
}
