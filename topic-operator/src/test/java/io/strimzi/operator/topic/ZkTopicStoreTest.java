/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.zk.Zk;
import io.strimzi.test.EmbeddedZooKeeper;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@Disabled
@ExtendWith(VertxExtension.class)
public class ZkTopicStoreTest {

    private EmbeddedZooKeeper zkServer;

    private static Vertx vertx;

    private ZkTopicStore store;
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
        this.store = new ZkTopicStore(zk, "/strimzi/topics");
    }

    @AfterEach
    public void teardown(VertxTestContext context) {
        Checkpoint async = context.checkpoint();
        zk.disconnect(ar -> async.flag());
        if (this.zkServer != null) {
            this.zkServer.close();
        }
    }

    @Test
    public void testCrud(VertxTestContext context) {
        Checkpoint async = context.checkpoint();

        Topic topic = new Topic.Builder("my_topic", 2,
                (short) 3, Collections.singletonMap("foo", "bar")).build();

        Promise<Void> failedCreateCompleted = Promise.promise();

        // Create the topic
        store.create(topic)
            .onComplete(context.succeeding())

            // Read the topic
            .compose(v -> store.read(new TopicName("my_topic")))
            .onComplete(context.succeeding(readTopic -> context.verify(() -> {
                // assert topics equal
                assertThat(readTopic.getTopicName(), is(topic.getTopicName()));
                assertThat(readTopic.getNumPartitions(), is(topic.getNumPartitions()));
                assertThat(readTopic.getNumReplicas(), is(topic.getNumReplicas()));
                assertThat(readTopic.getConfig(), is(topic.getConfig()));
            })))

            // try to create it again: assert an error
            .compose(v -> store.create(topic))
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(TopicStore.EntityExistsException.class));
                failedCreateCompleted.complete();
            })));

        Topic updatedTopic = new Topic.Builder(topic)
                .withNumPartitions(3)
                .withConfigEntry("fruit", "apple")
                .build();

        failedCreateCompleted.future()
            .compose(v -> {
                // update my_topic
                return store.update(updatedTopic);
            })
            .onComplete(context.succeeding())

            // re-read it and assert equal
            .compose(v -> store.read(new TopicName("my_topic")))
            .onComplete(context.succeeding(rereadTopic -> context.verify(() -> {
                // assert topics equal
                assertThat(rereadTopic.getTopicName(), is(updatedTopic.getTopicName()));
                assertThat(rereadTopic.getNumPartitions(), is(updatedTopic.getNumPartitions()));
                assertThat(rereadTopic.getNumReplicas(), is(updatedTopic.getNumReplicas()));
                assertThat(rereadTopic.getConfig(), is(updatedTopic.getConfig()));
            })))

            // delete it
            .compose(v -> store.delete(updatedTopic.getTopicName()))
            .onComplete(context.succeeding())

            // assert we can't read it again
            .compose(v -> store.read(new TopicName("my_topic")))
            .onComplete(context.succeeding(deletedTopic -> context.verify(() -> {
                assertThat(deletedTopic, is(nullValue()));
            })))

            // delete it again: assert an error
            .compose(v -> store.delete(updatedTopic.getTopicName()))
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(TopicStore.NoSuchEntityExistsException.class));
                async.flag();
            })));
    }

}
