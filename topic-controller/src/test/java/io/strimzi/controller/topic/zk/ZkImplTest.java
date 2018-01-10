/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.topic.zk;

import io.strimzi.controller.topic.EmbeddedZooKeeper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@RunWith(VertxUnitRunner.class)
public class ZkImplTest {

    private EmbeddedZooKeeper zkServer;

    private Vertx vertx = Vertx.vertx();

    @Before
    public void setup()
            throws IOException, InterruptedException,
            TimeoutException, ExecutionException {
        this.zkServer = new EmbeddedZooKeeper();

    }

    @After
    public void teardown() {
        if (this.zkServer != null) {
            this.zkServer.close();
        }
        vertx.close();
    }

    /**
     * When we get disconnected via a call to disconnect, can we use the
     * disconnectionHandler to reconnect?
     * @param context
     */
    @Test
    public void testConnectDisconnect(TestContext context) {
        ZkImpl zkImpl = new ZkImpl(vertx, zkServer.getZkConnectString(), 60_000);
        Async connection = context.async(10);
        final Handler<AsyncResult<Zk>> connectionHandler = ar -> {
            if (ar.succeeded()) {
                connection.countDown();
                zkImpl.disconnect((ar2)->{});
            } else {
                context.fail(ar.cause());
            }
        };
        zkImpl.disconnectionHandler(ar-> {
            System.err.println("Reconnecting " + connection.count());
            if (ar.succeeded()) {
                if (connection.count() > 0) {
                    System.err.println("Reconnecting " + connection.count());
                    zkImpl.connect(connectionHandler);
                } else {
                    context.fail(ar.cause());
                }
            }
        }).connect(connectionHandler);
        connection.await();
    }

    @Test
    public void testReconnectOnBounce(TestContext context) {
        ZkImpl zkImpl = new ZkImpl(vertx, zkServer.getZkConnectString(), 60_000);
        Async connection = context.async(10);
        final Handler<AsyncResult<Zk>> connectionHandler = ar -> {
            if (ar.succeeded()) {
                connection.countDown();
                try {
                    zkServer.restart();
                } catch (IOException e) {
                    context.fail(e);
                } catch (InterruptedException e) {
                    context.fail(e);
                }
            } else {
                context.fail(ar.cause());
            }
        };
        zkImpl.disconnectionHandler(ar-> {
            System.err.println("Reconnecting " + connection.count());
            if (ar.succeeded()) {
                if (connection.count() > 0) {
                    System.err.println("Reconnecting " + connection.count());
                    zkImpl.connect(connectionHandler);
                } else {
                    context.fail(ar.cause());
                }
            }
        }).connect(connectionHandler);
        connection.await();
    }

    private ZkImpl connect(TestContext context) {
        Async async = context.async();
        Zk zk = new ZkImpl(vertx, zkServer.getZkConnectString(), 60_000).connect(ar -> {
            if (ar.failed()) {
                context.fail(ar.cause());
                return;
            }
            async.complete();
        });

        async.await();
        return (ZkImpl)zk;
    }

    /**
     * Register a watch of /foo, check it is notified when we add /foo/bar
     * and again when we add /foo/baz.
     */
    @Test
    public void testWatchChildren(TestContext context) {
        Zk zk = connect(context);
        Async async2 = context.async(3);
        zk.create("/foo", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ar-> {
            context.assertTrue(ar.succeeded());
            zk.children("/foo", true, childResult -> {
                context.assertTrue(childResult.succeeded());
                if (async2.count() == 3) {
                    // first time
                    context.assertTrue(childResult.result().isEmpty());
                    zk.create("/foo/bar", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, createResult -> {
                        context.assertTrue(createResult.succeeded());
                    });
                    async2.countDown();
                } else if (async2.count() == 2) {
                    context.assertEquals(singletonList("bar"), childResult.result());
                    zk.create("/foo/baz", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, createResult -> {
                        context.assertTrue(createResult.succeeded());
                    });
                    async2.countDown();
                } else if (async2.count() == 1) {
                    context.assertEquals(new HashSet(asList("baz", "bar")), new HashSet(childResult.result()));
                    async2.countDown();
                } else {
                    context.fail();
                }
            });
        });
    }

    @Test
    public void testWatchUnwatchChildren(TestContext context) {
        ZkImpl zk = connect(context);
        // Create a node
        Async fooFuture = context.async();
        zk.create("/foo", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ar-> {
            fooFuture.complete();
        });
        fooFuture.await();

        // Now watch its children
        Async barFuture = context.async();
        zk.watchChildren("/foo", watchResult -> {
            context.assertEquals(singletonList("bar"), watchResult.result());
            zk.unwatchChildren("/foo");
            zk.delete("/foo/bar", -1, deleteResult -> {
                barFuture.countDown();
            });

        });
        zk.children("/foo", lsResult -> {
            context.assertEquals(emptyList(), lsResult.result());
            zk.create("/foo/bar", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ig -> {});
        });
        barFuture.await();
    }

    @Test
    public void testWatchUnwatchData(TestContext context) {
        ZkImpl zk = connect(context);
        // Create a node
        Async fooFuture = context.async();
        byte[] data1 = new byte[]{1};
        zk.create("/foo", data1, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ar-> {
            fooFuture.complete();
        });
        fooFuture.await();

        Async done = context.async();
        byte[] data2 = {2};
        zk.watchData("/foo", dataWatch -> {
            context.assertTrue(Arrays.equals(data2, dataWatch.result()));
        }).getData("/foo", dataResult -> {
            context.assertTrue(Arrays.equals(data1, dataResult.result()));

            zk.setData("/foo", data2, -1, setResult -> {
                done.complete();
            });
        });
    }

    @Test
    public void testWatchUnwatchExists(TestContext context) {
        ZkImpl zk = connect(context);
        // Create a node
        Async created = context.async(2);
        Async deleted = context.async(2);
        zk.watchExists("/foo", existsResult-> {
            if (existsResult.result() != null) {
                created.countDown();
            } else {
                deleted.countDown();
            }
        }).exists("/foo", null);
        zk.create("/foo", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ar-> {
            created.countDown();
        });
        created.await();

        zk.delete("/foo", -1, deleteResult -> {
            deleted.countDown();
        });
        deleted.await();

        zk.unwatchExists("/foo");
        Async created2 = context.async();
        zk.create("/foo", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ar-> {
            created2.complete();
        });
    }

    /**
     * Get the children of /foo; check it is NOT notified when we add /foo/bar.
     */
    @Test
    public void testGetChildren(TestContext context) {
        Zk zk = connect(context);
        Async async2 = context.async();
        zk.create("/foo", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, ar-> {
            context.assertTrue(ar.succeeded());
            zk.children("/foo", false, childResult -> {
                context.assertTrue(childResult.succeeded());
                if (async2.count() == 1) {
                    // first time
                    context.assertTrue(childResult.result().isEmpty());
                    zk.create("/foo/bar", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, createResult -> {
                        context.assertTrue(createResult.succeeded());
                    });
                    async2.complete();
                } else {
                    context.fail();
                }
            });
        });

        async2.await();
        Async async3 = context.async();
        zk.children("/foo", false, childResult -> {
            context.assertTrue(childResult.succeeded());
            if (async3.count() == 1) {
                // first time
                context.assertEquals(singletonList("bar"), childResult.result());
                zk.create("/foo/baz", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT, createResult -> {
                    context.assertTrue(createResult.succeeded());
                });
                async3.complete();
            } else {
                context.fail();
            }
        });
    }

    /**
     * Register a watch of /foo, check it is notified when we change its data.
     */
    @Test
    public void testWatchData(TestContext context) {
        Zk zk = connect(context);
        Async async2 = context.async(3);
        zk.create("/foo", null, AclBuilder.PUBLIC, CreateMode.PERSISTENT,  ar -> {
            context.assertTrue(ar.succeeded());
            zk.getData("/foo", true, dataResult -> {
                context.assertTrue(dataResult.succeeded());
                if (async2.count() == 3) {
                    context.assertTrue(dataResult.result().length == 0);
                    zk.setData("/foo", new byte[]{(byte) 1}, -1, setResult -> {
                        context.assertTrue(setResult.succeeded());
                    });
                    async2.countDown();
                } else if (async2.count() == 2) {
                    context.assertTrue(Arrays.equals(new byte[]{1}, dataResult.result()));
                    zk.setData("/foo", new byte[]{(byte) 2}, -1, setResult -> {
                        context.assertTrue(setResult.succeeded());
                    });
                    async2.countDown();
                } else if (async2.count() == 1) {
                    context.assertTrue(Arrays.equals(new byte[]{2}, dataResult.result()));
                    async2.countDown();
                } else {
                    context.fail();
                }
            });
        });
    }



}
