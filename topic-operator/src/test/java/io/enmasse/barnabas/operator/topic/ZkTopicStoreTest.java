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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class ZkTopicStoreTest {

    private EmbeddedZooKeeper zkServer;

    private ZkTopicStore store;

    @Before
    public void setupZooKeeper()
            throws IOException, InterruptedException,
            TimeoutException, ExecutionException {
        this.zkServer = new EmbeddedZooKeeper();
        CompletableFuture<Void> future = new CompletableFuture<>();
        ZooKeeper zk = new ZooKeeper(zkServer.getZkConnectString(), 60000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                switch (event.getState()) {
                    case SyncConnected:
                        future.complete(null);
                }
            }
        });

        this.store = new ZkTopicStore(new Supplier<Future<ZooKeeper>>() {
            @Override
            public Future<ZooKeeper> get() {
                return future.thenApply((v) -> zk);
            }
        });
    }

    @After
    public void shutdownZooKeeper() {
        if (this.zkServer != null) {
            this.zkServer.close();
        }
    }

    @Test
    public void testCrud() throws ExecutionException, InterruptedException {
        Topic topic = new Topic.Builder("my_topic", 2,
                (short)3, Collections.singletonMap("foo", "bar")).build();

        // Create the topic
        store.create(topic).get();

        // Read the topic
        Topic readTopic = store.read(new TopicName("my_topic")).get();

        // assert topics equal
        assertEquals(topic.getTopicName(), readTopic.getTopicName());
        assertEquals(topic.getNumPartitions(), readTopic.getNumPartitions());
        assertEquals(topic.getNumReplicas(), readTopic.getNumReplicas());
        assertEquals(topic.getConfig(), readTopic.getConfig());

        // try to create it again: assert an error
        try {
            store.create(topic).get();
            fail("Should throw");
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof KeeperException.NodeExistsException)) {
                throw e;
            }
        }

        // update my_topic
        Topic updated = new Topic.Builder(topic)
                .withNumPartitions(3)
                .withConfigEntry("fruit", "apple").build();
        store.update(updated);

        // re-read it and assert equal
        Topic rereadTopic = store.read(new TopicName("my_topic")).get();

        // assert topics equal
        assertEquals(updated.getTopicName(), rereadTopic.getTopicName());
        assertEquals(updated.getNumPartitions(), rereadTopic.getNumPartitions());
        assertEquals(updated.getNumReplicas(), rereadTopic.getNumReplicas());
        assertEquals(updated.getConfig(), rereadTopic.getConfig());

        // delete it
        store.delete(updated.getTopicName()).get();

        // assert we can't read it again
        try {
            store.read(new TopicName("my_topic")).get();
            fail("Should throw");
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof KeeperException.NoNodeException)) {
                throw e;
            }
        }

        // delete it again: assert an error
        try {
            store.delete(updated.getTopicName()).get();
            fail("Should throw");
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof KeeperException.NoNodeException)) {
                throw e;
            }
        }
    }

}
