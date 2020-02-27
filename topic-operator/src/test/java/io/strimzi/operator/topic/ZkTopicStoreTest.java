/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.zk.Zk;
import io.strimzi.test.EmbeddedZooKeeper;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@Disabled
@ExtendWith(VertxExtension.class)
public class ZkTopicStoreTest {

    private EmbeddedZooKeeper zkServer;

    private Vertx vertx = Vertx.vertx();

    private ZkTopicStore store;
    private Zk zk;

    @BeforeEach
    public void setup()
            throws IOException, InterruptedException {
        this.zkServer = new EmbeddedZooKeeper();
        zk = Zk.createSync(vertx, zkServer.getZkConnectString(), 60_000, 10_000);
        this.store = new ZkTopicStore(zk, "/strimzi/topics");
    }

    @AfterEach
    public void teardown(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        zk.disconnect(ar -> async.flag());
        if (this.zkServer != null) {
            this.zkServer.close();
        }
        vertx.close();
    }

    @Test
    public void testCrud(VertxTestContext context) throws ExecutionException, InterruptedException {
        Topic topic = new Topic.Builder("my_topic", 2,
                (short) 3, Collections.singletonMap("foo", "bar")).build();

        // Create the topic
        Checkpoint async0 = context.checkpoint();
        store.create(topic).setHandler(ar -> {
            async0.flag();
        });

        // Read the topic
        Checkpoint async1 = context.checkpoint();
        Future<Topic> topicFuture = Future.future();
        store.read(new TopicName("my_topic")).setHandler(ar -> {
            topicFuture.complete(ar.result());
            async1.flag();
        });
        Topic readTopic = topicFuture.result();

        // assert topics equal
        assertThat(readTopic.getTopicName(), is(topic.getTopicName()));
        assertThat(readTopic.getNumPartitions(), is(topic.getNumPartitions()));
        assertThat(readTopic.getNumReplicas(), is(topic.getNumReplicas()));
        assertThat(readTopic.getConfig(), is(topic.getConfig()));

        // try to create it again: assert an error
        store.create(topic).setHandler(ar -> {
            if (ar.succeeded()) {
                context.failNow(new Throwable("Should throw"));
            } else {
                if (!(ar.cause() instanceof TopicStore.EntityExistsException)) {
                    context.failNow(ar.cause());
                }
            }
        });

        // update my_topic
        Checkpoint async2 = context.checkpoint();
        Topic updated = new Topic.Builder(topic)
                .withNumPartitions(3)
                .withConfigEntry("fruit", "apple").build();
        store.update(updated).setHandler(ar -> async2.flag());

        // re-read it and assert equal
        Checkpoint async3 = context.checkpoint();
        Future<Topic> fut = Future.future();
        store.read(new TopicName("my_topic")).setHandler(ar -> {
            fut.complete(ar.result());
            async3.flag();
        });
        Topic rereadTopic = fut.result();

        // assert topics equal
        assertThat(rereadTopic.getTopicName(), is(updated.getTopicName()));
        assertThat(rereadTopic.getNumPartitions(), is(updated.getNumPartitions()));
        assertThat(rereadTopic.getNumReplicas(), is(updated.getNumReplicas()));
        assertThat(rereadTopic.getConfig(), is(updated.getConfig()));

        // delete it
        Checkpoint async4 = context.checkpoint();
        store.delete(updated.getTopicName()).setHandler(ar -> async4.flag());

        // assert we can't read it again
        Checkpoint async5 = context.checkpoint();
        store.read(new TopicName("my_topic")).setHandler(ar -> {
            async5.flag();
            if (ar.succeeded()) {
                context.verify(() -> assertThat(ar.result(), is(nullValue())));
            } else {
                context.failNow(new Throwable("read() on a non-existent topic should return null"));
            }
        });

        // delete it again: assert an error
        Checkpoint async6 = context.checkpoint();
        store.delete(updated.getTopicName()).setHandler(ar -> {
            async6.flag();
            if (ar.succeeded()) {
                context.failNow(new Throwable("Should throw"));
            } else {
                if (!(ar.cause() instanceof TopicStore.NoSuchEntityExistsException)) {
                    context.failNow(new Throwable("Unexpected exception " + ar.cause()));
                }
            }
        });
    }

}
