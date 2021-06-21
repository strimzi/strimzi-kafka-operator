/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.vertx.core.Promise;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public abstract class TopicStoreTestBase {

    protected TopicStore store;

    protected abstract boolean canRunTest();

    @Test
    public void testCrud(VertxTestContext context) {
        Assumptions.assumeTrue(canRunTest());

        String topicName = "my_topic_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        Topic topic = new Topic.Builder(topicName, 2,
                (short) 3, Collections.singletonMap("foo", "bar")).build();

        Promise<Void> failedCreateCompleted = Promise.promise();

        // Create the topic
        store.create(topic)
            .onComplete(context.succeeding())

            // Read the topic
            .compose(v -> store.read(new TopicName(topicName)))
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
                // update my_topic
                .compose(v -> store.update(updatedTopic))
            .onComplete(context.succeeding())

            // re-read it and assert equal
            .compose(v -> store.read(new TopicName(topicName)))
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
                .compose(v -> store.read(new TopicName(topicName)))
                .onComplete(context.succeeding(deletedTopic -> context.verify(() ->
                        assertThat(deletedTopic, is(nullValue()))))
                )

            // delete it again: assert an error
            .compose(v -> store.delete(updatedTopic.getTopicName()))
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(TopicStore.NoSuchEntityExistsException.class));
                context.completeNow();
            })));
    }

}
