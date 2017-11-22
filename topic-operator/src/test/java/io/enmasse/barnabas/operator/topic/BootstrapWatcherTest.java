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

package io.enmasse.barnabas.operator.topic;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RunWith(VertxUnitRunner.class)
public class BootstrapWatcherTest {

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

    @Test
    public void testConnectDisconnect(TestContext context) {
        Async async = context.async();
        Async disAsync = context.async();
        BootstrapWatcher watcher = new BootstrapWatcher(vertx,
                zkServer.getZkConnectString(),
                connected-> {
                    async.complete();
                }, disconnected-> {
                    disAsync.complete();
        });

        watcher.close();

    }
}
