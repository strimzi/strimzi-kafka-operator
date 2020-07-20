/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Migration tool to move ZkTopicStore to KafkaStreamsTopicStore.
 */
public class Zk2KafkaStreams {

    public static CompletionStage<KafkaStreamsTopicStoreService> upgrade(
            Zk zk,
            Config config,
            Properties kafkaProperties,
            boolean doStop
    ) {
        String topicsPath = config.get(Config.TOPICS_PATH);
        TopicStore zkTopicStore = new ZkTopicStore(zk, topicsPath);

        KafkaStreamsTopicStoreService service = new KafkaStreamsTopicStoreService();
        return service.start(config, kafkaProperties)
                .thenCompose(ksTopicStore -> {
                    CompletableFuture<Void> cf = new CompletableFuture<>();
                    zk.children(topicsPath, result -> {
                        if (result.failed()) {
                            cf.completeExceptionally(result.cause());
                        } else {
                            result.map(list -> {
                                @SuppressWarnings("rawtypes")
                                List<Future> results = new ArrayList<>();
                                list.forEach(topicName -> {
                                    Future<Topic> ft = zkTopicStore.read(new TopicName(topicName));
                                    results.add(
                                            ft.onSuccess(ksTopicStore::create)
                                                    .onSuccess(t1 -> zkTopicStore.delete(t1.getTopicName()))
                                    );
                                });
                                return CompositeFuture.all(results);
                            }).result().onComplete(ar -> {
                                if (ar.failed()) {
                                    cf.completeExceptionally(ar.cause());
                                } else {
                                    zk.delete(topicsPath, -1, v -> {
                                        if (v.failed()) {
                                            cf.completeExceptionally(v.cause());
                                        } else {
                                            cf.complete(null);
                                        }
                                    });
                                }
                            });
                        }
                    });
                    return cf;
                })
                .whenComplete((v, t) -> {
                    if (doStop || t != null) {
                        service.stop();
                    }
                })
                .thenApply(v -> service);
    }
}
