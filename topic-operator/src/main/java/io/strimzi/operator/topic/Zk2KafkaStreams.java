/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Migration tool to move ZkTopicStore to KafkaStreamsTopicStore.
 */
public class Zk2KafkaStreams {
    private static final Logger LOGGER = LoggerFactory.getLogger(Zk2KafkaStreams.class);

    protected static CompletionStage<KafkaStreamsTopicStoreService> upgrade(
            Zk zk,
            Config config,
            Properties kafkaProperties,
            boolean doStop
    ) {
        String topicsPath = config.get(Config.TOPICS_PATH);

        LOGGER.info("Upgrading topic store [{}]: {}", doStop, topicsPath);

        TopicStore zkTopicStore = new TempZkTopicStore(zk, topicsPath);
        KafkaStreamsTopicStoreService service = new KafkaStreamsTopicStoreService();
        return service.start(config, kafkaProperties)
                .thenCompose(ksTopicStore -> {
                    LOGGER.info("Starting upgrade ...");
                    @SuppressWarnings("rawtypes")
                    List<Future> results = new ArrayList<>();
                    List<String> list = zk.getChildren(topicsPath);
                    LOGGER.info("Topics to upgrade: {}", list);
                    list.forEach(topicName -> {
                        TopicName tn = new TopicName(topicName);
                        Future<Topic> ft = zkTopicStore.read(tn);
                        results.add(
                                // check if the topic already exists in the new KSTS
                                // only create if it doesn't, and do not update it with an old value
                                ft.compose(t -> ksTopicStore.read(tn).map(et -> new AbstractMap.SimpleImmutableEntry<>(t, et)))
                                        .compose(e -> e.getValue() == null ? ksTopicStore.create(e.getKey()) : Future.succeededFuture())
                                        .compose(v -> zkTopicStore.delete(tn))
                        );
                    });
                    CompletableFuture<Void> result = new CompletableFuture<>();
                    CompositeFuture cf = CompositeFuture.all(results);
                    cf.onComplete(ar -> {
                        if (ar.failed()) {
                            result.completeExceptionally(ar.cause());
                        } else {
                            result.complete(null);
                        }
                    });
                    return result;
                })
                .thenRun(() -> {
                    LOGGER.info("Deleting ZK topics path: {}", topicsPath);
                    zk.delete(topicsPath, -1);
                })
                .whenCompleteAsync((v, t) -> {
                    // stop in another thread
                    if (doStop || t != null) {
                        service.stop();
                    }
                    LOGGER.info("Upgrade complete", t);
                })
                .thenApply(v -> service);
    }
}
