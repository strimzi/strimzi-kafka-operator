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

package io.strimzi.controller.topic;

import io.strimzi.controller.topic.zk.ZkImpl;
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
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
public class ZkTopicStoreTest {

    private EmbeddedZooKeeper zkServer;

    private Vertx vertx = Vertx.vertx();

    private ZkTopicStore store;

    @Before
    public void setup()
            throws IOException, InterruptedException,
            TimeoutException, ExecutionException {
        this.zkServer = new EmbeddedZooKeeper();
        ZkImpl zk = new ZkImpl(vertx, zkServer.getZkConnectString(), 60000, false);
        this.store = new ZkTopicStore(zk, vertx);
    }

    @After
    public void teardown() {
        if (this.zkServer != null) {
            this.zkServer.close();
        }
        vertx.close();
    }

    @Test
    public void testCrud(TestContext context) throws ExecutionException, InterruptedException {
        Topic topic = new Topic.Builder("my_topic", 2,
                (short)3, Collections.singletonMap("foo", "bar")).build();



        // Create the topic
        Async async0 = context.async();
        store.create(topic, ar -> {
            async0.complete();
        });
        async0.await();

        // Read the topic
        Async async1 = context.async();
        Future<Topic> topicFuture = Future.future();
        store.read(new TopicName("my_topic"), ar -> {
            topicFuture.complete(ar.result());
            async1.complete();

        });
        async1.await();
        Topic readTopic = topicFuture.result();

        // assert topics equal
        assertEquals(topic.getTopicName(), readTopic.getTopicName());
        assertEquals(topic.getNumPartitions(), readTopic.getNumPartitions());
        assertEquals(topic.getNumReplicas(), readTopic.getNumReplicas());
        assertEquals(topic.getConfig(), readTopic.getConfig());

        // try to create it again: assert an error
        store.create(topic, ar-> {
            if (ar.succeeded()) {
                context.fail("Should throw");
            } else {
                if (!(ar.cause() instanceof TopicStore.EntityExistsException)) {
                    context.fail(ar.cause().toString());
                }
            }
        });

        // update my_topic
        Async async2 = context.async();
        Topic updated = new Topic.Builder(topic)
                .withNumPartitions(3)
                .withConfigEntry("fruit", "apple").build();
        store.update(updated, ar->async2.complete());
        async2.await();

        // re-read it and assert equal
        Async async3 = context.async();
        Future<Topic> fut = Future.future();
        store.read(new TopicName("my_topic"), ar -> {
            fut.complete(ar.result());
            async3.complete();
        });
        async3.await();
        Topic rereadTopic = fut.result();

        // assert topics equal
        assertEquals(updated.getTopicName(), rereadTopic.getTopicName());
        assertEquals(updated.getNumPartitions(), rereadTopic.getNumPartitions());
        assertEquals(updated.getNumReplicas(), rereadTopic.getNumReplicas());
        assertEquals(updated.getConfig(), rereadTopic.getConfig());

        // delete it
        Async async4 = context.async();
        store.delete(updated.getTopicName(), ar-> async4.complete());
        async4.await();

        // assert we can't read it again
        Async async5 = context.async();
        store.read(new TopicName("my_topic"), ar-> {
            async5.complete();
            if (ar.succeeded()) {
                context.assertNull(ar.result());
            } else {
                context.fail("read() on a non-existent topic should return null");
            }
        });
        async5.await();

        // delete it again: assert an error
        Async async6 = context.async();
        store.delete(updated.getTopicName(), ar-> {
            async6.complete();
            if (ar.succeeded()) {
                context.fail("Should throw");
            } else {
                if (!(ar.cause() instanceof TopicStore.NoSuchEntityExistsException)) {
                    context.fail("Unexpected exception "+ar.cause());
                }
            }
        });
        async6.await();
    }

}
