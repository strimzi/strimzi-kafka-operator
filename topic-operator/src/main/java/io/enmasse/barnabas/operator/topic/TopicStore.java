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

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CompletableFuture;

/**
 * Represents a persistent data store where the operator can store its copy of the
 * topic state that won't be modified by either K8S or Kafka.
 */
public interface TopicStore {
    /**
     * Get the topic with the given name.
     * @param name The name of the topic to get.
     * @return A future for the stored topic, which will return null if the topic didn't exist.
     */
    CompletableFuture<Topic> read(TopicName name);

    /**
     * Persist the given topic in the store.
     * @param topic The topic to persist.
     * @return A future for the success or failure of the operation.
     */
    CompletableFuture<Void> create(Topic topic);

    /**
     * Update the given topic in the store.
     * @param topic The topic to update.
     * @return A future for the success or failure of the operation.
     */
    CompletableFuture<Void> update(Topic topic);

    /**
     * Delete the given topic from the store.
     * @param topic The topic to delete.
     * @return A future for the success or failure of the operation.
     */
    CompletableFuture<Void> delete(Topic topic);
}

/**
 * Implementation of {@link TopicStore} that stores the topic state in ZooKeeper.
 */
class ZkTopicStore implements TopicStore {

    private final ZooKeeper zookeeper;

    public ZkTopicStore(ZooKeeper zookeeper) {
        this.zookeeper = zookeeper;
        createParent(zookeeper, "/barnabas");
        createParent(zookeeper, "/barnabas/topics");
    }

    private static void createParent(ZooKeeper zookeeper, String path) {
        try {
            zookeeper.create(path, null, null, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // That's fine then
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private static String getTopicPath(TopicName name) {
        return "/barnabas/topics/" + name;
    }

    @Override
    public CompletableFuture<Topic> read(TopicName topicName) {
        CompletableFuture<Topic> result = new CompletableFuture<>();
        zookeeper.getData(getTopicPath(topicName), null, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (rc == KeeperException.Code.OK.intValue()) {
                    result.complete(TopicSerialization.fromJson(data));
                } else {
                    result.completeExceptionally(KeeperException.create(rc));
                }
            }
        }, null);
        return result;
    }


    @Override
    public CompletableFuture<Void> create(Topic topic) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        byte[] data = TopicSerialization.toJson(topic);
        zookeeper.create(getTopicPath(topic.getTopicName()), data, null, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                if (rc == KeeperException.Code.OK.intValue()) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(KeeperException.create(rc));
                }
            }
        }, null);
        return result;
    }

    @Override
    public CompletableFuture<Void> update(Topic topic) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        byte[] data = TopicSerialization.toJson(topic);
        // TODO pass a non-zero version
        zookeeper.setData(getTopicPath(topic.getTopicName()), data, -1, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                if (rc == KeeperException.Code.OK.intValue()) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(KeeperException.create(rc));
                }
            }
        }, null);
        return result;
    }

    @Override
    public CompletableFuture<Void> delete(Topic topic) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        // TODO pass a non-zero version
        zookeeper.delete(getTopicPath(topic.getTopicName()), -1, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (rc == KeeperException.Code.OK.intValue()) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(KeeperException.create(rc));
                }
            }
        }, null);
        return result;
    }
}
