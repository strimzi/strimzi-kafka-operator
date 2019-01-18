/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.model.KafkaResources;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.strimzi.api.kafka.model.KafkaResources.externalBootstrapServiceName;
import static org.junit.jupiter.api.Assertions.fail;

public class AvailabilityVerifier {

    private static final Logger LOGGER = LogManager.getLogger(AvailabilityVerifier.class);

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

    public AvailabilityVerifier(KubernetesClient client, String namespace, String clusterName, String userName) {
        producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                getExternalBootstrapConnect(client, namespace, clusterName));
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        producerProperties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        producerProperties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, userName + "-producer");

        consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                "my-group-" + new Random().nextInt(Integer.MAX_VALUE));
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                getExternalBootstrapConnect(client, namespace, clusterName));
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProperties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        consumerProperties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, userName + "-consumer");

        try {
            String tsPassword = "foo";
            File tsFile = File.createTempFile(getClass().getName(), ".truststore");
            tsFile.deleteOnExit();
            KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
            ts.load(null, tsPassword.toCharArray());
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            for (Map.Entry<String, String> entry : client.secrets().inNamespace(namespace).withName(KafkaResources.clusterCaCertificateSecretName(clusterName)).get().getData().entrySet()) {
                String clusterCaCert = entry.getValue();
                Certificate cert = cf.generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(clusterCaCert)));
                ts.setCertificateEntry(entry.getKey(), cert);
            }
            FileOutputStream tsOs = new FileOutputStream(tsFile);
            try {
                ts.store(tsOs, tsPassword.toCharArray());
            } finally {
                tsOs.close();
            }
            producerProperties.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, KeyStore.getDefaultType());
            consumerProperties.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, KeyStore.getDefaultType());
            producerProperties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, tsPassword);
            consumerProperties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, tsPassword);
            producerProperties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, tsFile.getAbsolutePath());
            consumerProperties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, tsFile.getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {

            Secret userSecret = client.secrets().inNamespace(namespace).withName(userName).get();

            String clientsCaCert = userSecret.getData().get("ca.crt");
            LOGGER.info(clientsCaCert);

            String userCaCert = userSecret.getData().get("user.crt");
            String userCaKey = userSecret.getData().get("user.key");
            String ksPassword = "foo";
            producerProperties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ksPassword);
            consumerProperties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ksPassword);
            LOGGER.info(userCaCert);
            LOGGER.info(userCaKey);
            File ksFile = createKeystore(Base64.getDecoder().decode(clientsCaCert),
                    Base64.getDecoder().decode(userCaCert),
                    Base64.getDecoder().decode(userCaKey),
                    ksPassword);
            producerProperties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ksFile.getAbsolutePath());
            consumerProperties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ksFile.getAbsolutePath());

            producerProperties.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
            consumerProperties.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private File createKeystore(byte[] ca, byte[] cert, byte[] key, String password) throws IOException, InterruptedException {
        File caFile = File.createTempFile(getClass().getName(), ".crt");
        caFile.deleteOnExit();
        Files.write(caFile.toPath(), ca);
        File certFile = File.createTempFile(getClass().getName(), ".crt");
        certFile.deleteOnExit();
        Files.write(certFile.toPath(), cert);
        File keyFile = File.createTempFile(getClass().getName(), ".key");
        keyFile.deleteOnExit();
        Files.write(keyFile.toPath(), key);
        File keystore = File.createTempFile(getClass().getName(), ".keystore");
        keystore.delete(); // Note horrible race condition, but this is only for testing
        //keystore.deleteOnExit();
        // RANDFILE=/tmp/.rnd openssl pkcs12 -export -in $3 -inkey $4 -name $HOSTNAME -password pass:$2 -out $1
        if (new ProcessBuilder("openssl",
                "pkcs12",
                "-export",
                "-in", certFile.getAbsolutePath(),
                "-inkey", keyFile.getAbsolutePath(),
                "-chain",
                "-CAfile", caFile.getAbsolutePath(),
                "-name", "dfbdbd",
                "-password", "pass:" + password,
                "-out", keystore.getAbsolutePath()).inheritIO().start().waitFor() != 0) {
            fail();
        }
        keystore.deleteOnExit();
        return keystore;
    }

    private static String getExternalBootstrapConnect(KubernetesClient client, String namespace, String clusterName) {
        if (client.isAdaptable(OpenShiftClient.class)) {
            Route route = client.adapt(OpenShiftClient.class).routes().inNamespace(namespace).withName(clusterName + "-kafka-bootstrap").get();
            if (route != null && !route.getStatus().getIngress().isEmpty()) {
                return route.getStatus().getIngress().get(0).getHost() + ":443";
            }
        }

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
        return result + ":9094";
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
        double targetRate = 50;
        this.producer = new KafkaProducer<>(producerProperties);
        this.sender = new Thread(() -> {
            long msgId = 0;
            Map<Class, Integer> producerErrors = new HashMap<>();
            long sent = 0;
            long sent0 = 0;
            long t0 = System.nanoTime();
            while (go) {
                try {
                    producer.send(new ProducerRecord<>("my-topic", msgId++, System.nanoTime()), (recordMeta, error) -> {
                        if (error != null) {
                            incrementErrCount(error, producerErrors);
                        }
                    });
                    sent++;
                    sent0++;
                } catch (Exception e) {
                    incrementErrCount(e, producerErrors);
                }
                Result result = AvailabilityVerifier.this.producerStats;
                AvailabilityVerifier.this.producerStats = null;
                if (result != null) {
                    result.publishProducerStats(sent, producerErrors);
                }
                double rate = ((double) sent0) / ((System.nanoTime() - t0) / 1e9);
                if (rate > targetRate) {
                    while (rate > targetRate) {
                        // spin!
                        rate = ((double) sent0) / ((System.nanoTime() - t0) / 1e9);
                    }
                    sent0 = 0;
                    t0 = System.nanoTime();
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
            return maxLatencyNs / 1e6;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "runtime/s=" + runtimeSeconds() +
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
            return runtimeNs / 1e9;
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
