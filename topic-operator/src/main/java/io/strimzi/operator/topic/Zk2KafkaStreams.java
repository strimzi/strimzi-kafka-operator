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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Migration tool to move ZkTopicStore to KafkaStreamsTopicStore.
 */
public class Zk2KafkaStreams {
    private static final Logger log = LoggerFactory.getLogger(Zk2KafkaStreams.class);

    public static CompletionStage<KafkaStreamsTopicStoreService> upgrade(
            Zk zk,
            Config config,
            Properties kafkaProperties,
            boolean doStop
    ) {
        String topicsPath = config.get(Config.TOPICS_PATH);

        log.info("Upgrading topic store [{}]: {}", doStop, topicsPath);

        TopicStore zkTopicStore = new TempZkTopicStore(zk, topicsPath);
        KafkaStreamsTopicStoreService service = new KafkaStreamsTopicStoreService();
        return service.start(config, kafkaProperties)
                .thenCompose(ksTopicStore -> {
                    log.info("Starting upgrade ...");
                    @SuppressWarnings("rawtypes")
                    List<Future> results = new ArrayList<>();
                    List<String> list = zk.getChildren(topicsPath);
                    log.info("Topics to upgrade: {}", list);
                    list.forEach(topicName -> {
                        TopicName tn = new TopicName(topicName);
                        Future<Topic> ft = zkTopicStore.read(tn);
                        results.add(ft.compose(ksTopicStore::create).compose(v -> zkTopicStore.delete(tn)));
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
                    log.info("Deleting ZK topics path: {}", topicsPath);
                    zk.delete(topicsPath, -1);
                })
                .whenCompleteAsync((v, t) -> {
                    // stop in another thread
                    if (doStop || t != null) {
                        service.stop();
                    }
                    log.info("Upgrade complete", t);
                })
                .thenApply(v -> service);
    }
}
