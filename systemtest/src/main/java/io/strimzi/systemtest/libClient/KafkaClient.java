/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.libClient;

import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.VertxFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

import io.vertx.core.Vertx;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.strimzi.api.kafka.model.KafkaResources.externalBootstrapServiceName;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaClient implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(KafkaClient.class);
    private final List<Vertx> clients = new ArrayList<>();
    private Properties clientProperties;

    private static final Config CONFIG = Config.autoConfigure(System.getenv().getOrDefault("TEST_CLUSTER_CONTEXT", null));
    protected static final DefaultKubernetesClient CLIENT = new DefaultKubernetesClient(CONFIG);

    public KafkaClient() {
    }

    public Properties getClientProperties() {
        return clientProperties;
    }

    public KafkaClient setClientProperties(Properties clientProperties) {
        this.clientProperties = clientProperties;
        return this;
    }

    @Override
    public void close() throws Exception {
        for (Vertx client : clients) {
            client.close();
        }
    }

    public Future<Integer> sendMessages(String topicName, String namespace, String clusterName, String userName, int messageCount) {

        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        Vertx vertx = VertxFactory.create();
        clients.add(vertx);

        vertx.deployVerticle(new Producer(createProducerProperties(namespace, clusterName, userName), resultPromise, messageCount, topicName));

        try {
            resultPromise.get(2, TimeUnit.MINUTES);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        return resultPromise;
    }

    public Future<Integer> receiveMessages(String topicName, String namespace, String clusterName, String userName, int messageCount) {

        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        Vertx vertx = VertxFactory.create();
        clients.add(vertx);

        vertx.deployVerticle(new Consumer(createConsumerProperties(namespace, clusterName, userName), resultPromise, messageCount, topicName));

        try {
            resultPromise.get(2, TimeUnit.MINUTES);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        return resultPromise;
    }

    private Properties createProducerProperties(String namespace, String clusterName, String userName) {
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                getExternalBootstrapConnect(namespace, clusterName));
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        producerProperties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        producerProperties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, userName + "-producer");
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        producerProperties.putAll(sharedClientProperties(namespace, clusterName, userName));

        return producerProperties;
    }

    private Properties createConsumerProperties(String namespace, String clusterName, String userName) {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                "my-group-" + new Random().nextInt(Integer.MAX_VALUE));
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                getExternalBootstrapConnect(namespace, clusterName));
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        consumerProperties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, userName + "-consumer");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumerProperties.putAll(sharedClientProperties(namespace, clusterName, userName));

        return consumerProperties;
    }

    private Properties sharedClientProperties(String namespace, String clusterName, String userName) {
        Properties properties = new Properties();

        try {
            String tsPassword = "foo";
            File tsFile = File.createTempFile(getClass().getName(), ".truststore");
            tsFile.deleteOnExit();
            KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
            ts.load(null, tsPassword.toCharArray());
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            for (Map.Entry<String, String> entry : CLIENT.secrets().inNamespace(namespace).withName(KafkaResources.clusterCaCertificateSecretName(clusterName)).get().getData().entrySet()) {
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
            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, KeyStore.getDefaultType());
            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, tsPassword);
            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, tsFile.getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
//        try {
//
//            Secret userSecret = CLIENT.secrets().inNamespace(namespace).withName(userName).get();
//
//            String clientsCaCert = userSecret.getData().get("ca.crt");
//            LOGGER.info(clientsCaCert);
//
//            String userCaCert = userSecret.getData().get("user.crt");
//            String userCaKey = userSecret.getData().get("user.key");
//            String ksPassword = "foo";
//            properties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ksPassword);
//            LOGGER.info(userCaCert);
//            LOGGER.info(userCaKey);
//            File ksFile = createKeystore(Base64.getDecoder().decode(clientsCaCert),
//                    Base64.getDecoder().decode(userCaCert),
//                    Base64.getDecoder().decode(userCaKey),
//                    ksPassword);
//            properties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ksFile.getAbsolutePath());
//
//            properties.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }

        return properties;

    }

    private static String getExternalBootstrapConnect(String namespace, String clusterName) {
        if (CLIENT.isAdaptable(OpenShiftClient.class)) {
            Route route = CLIENT.adapt(OpenShiftClient.class).routes().inNamespace(namespace).withName(clusterName + "-kafka-bootstrap").get();
            if (route != null && !route.getStatus().getIngress().isEmpty()) {
                return route.getStatus().getIngress().get(0).getHost() + ":443";
            }
        }

        Service extBootstrapService = CLIENT.services()
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
}
