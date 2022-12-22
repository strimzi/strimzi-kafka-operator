/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.apicurio.registry.utils.streams.diservice.AsyncBiFunctionService;
import io.apicurio.registry.utils.streams.ext.ForeachActionDispatcher;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * We first register CompletableFuture,
 * which will be completed once Streams transformer handles CRUD command.
 */
public class WaitForResultService implements AsyncBiFunctionService.WithSerdes<String, String, Integer> {
    protected static final String NAME = "WaitForResultService";

    private final long timeoutMillis;
    private final Map<String, ResultCF> waitingResults = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executorService;

    protected WaitForResultService(long timeoutMillis, ForeachActionDispatcher<String, Integer> dispatcher) {
        this.timeoutMillis = timeoutMillis;
        dispatcher.register(this::topicUpdated);
        executorService = new ScheduledThreadPoolExecutor(1);
        executorService.scheduleAtFixedRate(this::checkStaleResults, timeoutMillis / 2, timeoutMillis / 2, TimeUnit.MILLISECONDS);
    }

    /**
     * Complete (with exception / error) any ResultCF that is older than timeout.
     * This way we don't block / hang the response in KafkaStreamsTopicStore for too long.
     */
    private void checkStaleResults() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, ResultCF>> iterator = waitingResults.entrySet().iterator();
        while (iterator.hasNext()) {
            ResultCF rcf = iterator.next().getValue();
            if (now - rcf.ts > timeoutMillis) {
                rcf.complete(KafkaStreamsTopicStore.toIndex(TopicStore.InvalidStateException.class));
                iterator.remove();
            }
        }
    }

    /**
     * Notification (from transformer)
     */
    private void topicUpdated(String uuid, Integer i) {
        CompletableFuture<Integer> cf = waitingResults.remove(uuid);
        if (cf != null) {
            cf.complete(i);
        }
    }

    /** Close the service */
    @Override
    public void close() {
        executorService.shutdown();
    }

    /**
     * @return Key serde of type string
     */
    @Override
    public Serde<String> keySerde() {
        return Serdes.String();
    }

    /**
     * @return Request Serde of type string
     */
    @Override
    public Serde<String> reqSerde() {
        return Serdes.String();
    }

    /**
     * @return Result serde of type integer
     */
    @Override
    public Serde<Integer> resSerde() {
        return Serdes.Integer();
    }

    /**
     * Applies the ResultCF in waitingResults map so it could be completed
     *
     * @param name  Name
     * @param uuid  The UUID associated to ResultCF
     * @return A ResultCF
     */
    @Override
    public CompletionStage<Integer> apply(String name, String uuid) {
        ResultCF cf = new ResultCF();
        waitingResults.put(uuid, cf);
        return cf;
    }

    private static class ResultCF extends CompletableFuture<Integer> {
        private final long ts;

        private ResultCF() {
            this.ts = System.currentTimeMillis();
        }
    }

}
