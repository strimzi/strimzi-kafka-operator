/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.strimzi.api.kafka.model.KafkaResources.externalBootstrapServiceName;

public class AvailabilityVerifier {

    private final Properties consumerProperties;
    private KafkaProducer<Long, Long> producer;
    private final Properties producerProperties;
    private Thread sender;

    private KafkaConsumer<Long, Long> consumer;
    private Thread receiver;

    private volatile boolean go = false;
    private volatile Result producerStats = null;
    private volatile Result consumerStats;
    private long startTime;

    public AvailabilityVerifier(KubernetesClient client, String namespace, String clusterName) {
        producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                getExternalBootstrapConnect(client, namespace, clusterName));
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());


        consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                "my-group-" + new Random().nextInt(Integer.MAX_VALUE));
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                getExternalBootstrapConnect(client, namespace, clusterName));
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

    }

    private static String getExternalBootstrapConnect(KubernetesClient client, String namespace, String clusterName) {
        Service extBootstrapService = client.services()
                .inNamespace(namespace)
                .withName(externalBootstrapServiceName(clusterName))
                .get();
        if (extBootstrapService == null) {
            throw new RuntimeException("Kafka cluster " + clusterName + " doesn't have an external bootstrap service");
        }
        LoadBalancerIngress loadBalancerIngress = extBootstrapService.getStatus().getLoadBalancer().getIngress().get(0);
        String result = loadBalancerIngress.getHostname();
        if (result == null) {
            result = loadBalancerIngress.getIp();
        }
        return result + ":" + 9094;
    }

    /**
     * Get some stats about sending and reveiving since {@link #start()}.
     */
    public synchronized Result stats() {
        if (!go) {
            throw new IllegalStateException();
        }
        Result result = new Result(1_000_000 * (System.currentTimeMillis() - startTime));
        this.producerStats = result;
        this.consumerStats = result;
        while (producerStats != null 
                || consumerStats != null) {
            // busy loop!
        }
        return result;
    }

    /**
     * Start sending and receiving messages.
     */
    public void start() {
        if (go) {
            throw new IllegalStateException();
        }
        go = true;
        this.producer = new KafkaProducer<>(producerProperties);
        this.sender = new Thread(() -> {
            long msgId = 0;
            Map<Class, Integer> producerErrors = new HashMap<>();
            long sent = 0;
            while (go) {
                try {
                    producer.send(new ProducerRecord<Long, Long>("my-topic", msgId++, System.nanoTime()), (recordMeta, error) -> {
                        if (error != null) {
                            incrementErrCount(error, producerErrors);
                        }
                    });
                    sent++;
                } catch (Exception e) {
                    incrementErrCount(e, producerErrors);
                }
                Result result = AvailabilityVerifier.this.producerStats;
                AvailabilityVerifier.this.producerStats = null;
                if (result != null) {
                    result.publishProducerStats(sent, producerErrors);
                }
            }
        });
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.receiver = new Thread(() -> {
            final Map<Class, Integer> consumerErrors = new HashMap<>();
            long received = 0;
            long maxLatencyNs = 0;
            consumer.subscribe(Collections.singleton("my-topic"));
            while (go) {
                try {
                    ConsumerRecords<Long, Long> records = consumer.poll(Duration.ofSeconds(1));
                    long t0 = System.nanoTime();
                    received += records.count();
                    for (ConsumerRecord<Long, Long> record : records) {
                        long msgId = record.key();
                        maxLatencyNs = Math.max(maxLatencyNs, t0 - record.value());
                    }
                } catch (Exception e) {
                    incrementErrCount(e, consumerErrors);
                }
                Result result = AvailabilityVerifier.this.consumerStats;
                AvailabilityVerifier.this.consumerStats = null;
                if (result != null) {
                    result.publishConsumerStats(received, maxLatencyNs, consumerErrors);
                }
            }
        });
        this.startTime = System.currentTimeMillis();
        this.sender.start();
        this.receiver.start();
    }

    private void incrementErrCount(Exception error, Map<Class, Integer> errors) {
        Class<? extends Exception> cls = error.getClass();
        Integer count = errors.getOrDefault(cls, 0);
        errors.put(cls, count + 1);
    }

    public static class Result {
        private final long runtimeNs;
        private Map<Class, Integer> producerErrors;
        private long sent;

        private Map<Class, Integer> consumerErrors;
        private long maxLatencyNs;
        private long received;

        private Result(long runtimeNs) {
            this.runtimeNs = runtimeNs;
        }

        public long sent() {
            return sent;
        }

        public long received() {
            return received;
        }

        public Map<Class, Integer> producerErrors() {
            return producerErrors;
        }

        public Map<Class, Integer> consumerErrors() {
            return consumerErrors;
        }

        public double maxLatencyMs() {
            return maxLatencyNs * 1e6;
        }

        @Override
        public String toString() {
            return "Result{" +
                    ", runtime/s=" + runtimeSeconds() +
                    ", sent=" + sent +
                    ", sendRate/s⁻¹=" + sendRatePerSecond() +
                    ", received=" + received +
                    ", receiveRate/s⁻¹=" + receiveRatePerSecond() +
                    ", maxLatency/ms=" + maxLatencyMs() +
                    ", producerErrors=" + producerErrors +
                    ", consumerErrors=" + consumerErrors +
                    '}';
        }

        private double sendRatePerSecond() {
            return sent() / runtimeSeconds();
        }

        private double receiveRatePerSecond() {
            return received() / runtimeSeconds();
        }

        private double runtimeSeconds() {
            return runtimeNs * 1e9;
        }

        public void publishConsumerStats(long received, long maxLatencyNs, Map<Class, Integer> hashMap) {
            this.received = received;
            this.maxLatencyNs = maxLatencyNs;
            this.consumerErrors = new HashMap(hashMap);
        }

        public void publishProducerStats(long sent, Map<Class, Integer> hashMap) {
            this.sent = sent;
            this.producerErrors = new HashMap(hashMap);
        }
    }

    /**
     * Stop sending and receiving
     * @param timeoutMs The maximum time to wait for the stopping to complete.
     * @return
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public Result stop(long timeoutMs) throws InterruptedException {
        long t0 = System.nanoTime();
        if (!go) {
            throw new IllegalStateException();
        }
        Result results = stats();
        this.go = false;
        this.sender.join(timeoutLeft(timeoutMs, t0));
        this.sender = null;
        producer.close(timeoutLeft(timeoutMs, t0), TimeUnit.MILLISECONDS);
        this.receiver.join(timeoutLeft(timeoutMs, t0));
        this.receiver = null;
        consumer.close(Duration.ofMillis(timeoutLeft(timeoutMs, t0)));
        return results;
    }

    private long timeoutLeft(long timeoutMs, long t0) {
        long l = timeoutMs - ((System.nanoTime() - t0) / 1_000_000);
        if (l <= 0) {
            throw new io.strimzi.test.TimeoutException("Timeout while stopping " + this);
        }
        return l;
    }
}
