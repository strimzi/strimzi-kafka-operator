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
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * Implementation of {@link TopicStore} that stores the topic state in ZooKeeper.
 */
public class ZkTopicStore implements TopicStore {

    private final static Logger logger = LoggerFactory.getLogger(ZkTopicStore.class);

    private final Supplier<Future<ZooKeeper>> zookeeper;

    private final ACL acl;

    public ZkTopicStore(Supplier<Future<ZooKeeper>> zookeeper) {
        this.zookeeper = zookeeper;
        acl = new ACL();
        String scheme = "world";
        String id = "anyone";
        int perm = ZooDefs.Perms.READ | ZooDefs.Perms.WRITE |ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE;
        acl.setId(new Id(scheme, id));
        acl.setPerms(perm);
        createParent("/barnabas");
        createParent("/barnabas/topics");
    }

    private ZooKeeper getZookeeper() {
        try {
            // TODO Can we improve this?
            // It blocks indefinitely waiting for a zookeeper connection
            // Since all the other methods return futures, we could let the
            // error propagate via their futures
            return this.zookeeper.get().get();
        } catch (Exception e) {
            logger.error("Error waiting for a ZooKeeper instance", e);
            if (e instanceof RuntimeException) {
                throw (RuntimeException)e;
            }
            throw new RuntimeException(e);
        }
    }

    private void createParent(String path) {
        try {
            getZookeeper().create(path, new byte[0], Collections.singletonList(acl), CreateMode.PERSISTENT);
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
        getZookeeper().getData(getTopicPath(topicName), null, new AsyncCallback.DataCallback() {
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
        getZookeeper().create(getTopicPath(topic.getTopicName()), data, Collections.singletonList(acl), CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
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
        getZookeeper().setData(getTopicPath(topic.getTopicName()), data, -1, new AsyncCallback.StatCallback() {
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
    public CompletableFuture<Void> delete(TopicName topicName) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        // TODO pass a non-zero version
        getZookeeper().delete(getTopicPath(topicName), -1, new AsyncCallback.VoidCallback() {
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
