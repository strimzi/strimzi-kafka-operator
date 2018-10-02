/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.ClusterOperator;
import io.strimzi.test.JUnitGroup;
import io.strimzi.test.Namespace;
import io.strimzi.test.StrimziRunner;
import io.strimzi.test.k8s.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;

@RunWith(StrimziRunner.class)
@Namespace(SecurityST.NAMESPACE)
@ClusterOperator
public class SecurityST extends AbstractST {

    public static final String NAMESPACE = "security-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(SecurityST.class);
    private static final String OPENSSL_RETURN_CODE = "Verify return code: 0 (ok)";
    private static final String TLS_PROTOCOL = "Protocol  : TLSv1";
    private static final String SSL_TIMEOUT = "Timeout   : 300 (sec)";

    @Test
    @JUnitGroup(name = "regression")
    public void testCertificates() {
        LOGGER.info("Running testCertificates {}", CLUSTER_NAME);
        resources().kafkaEphemeral(CLUSTER_NAME, 2).done();
        String commandForKafkaBootstrap = "openssl s_client -connect my-cluster-kafka-bootstrap:9093 -showcerts " +
                        "-CAfile /opt/kafka/broker-certs/cluster-ca.crt " +
                        "-verify_hostname my-cluster-kafka-bootstrap";
        String outputForKafkaBootstrap =
                kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, 0), "kafka", "/bin/bash", "-c", commandForKafkaBootstrap).out();
        checkKafkaCertificates(outputForKafkaBootstrap);


        String commandForZookeeperClient = "openssl s_client -connect my-cluster-zookeeper-client:2181 -showcerts " +
                "-CAfile /opt/kafka/broker-certs/cluster-ca.crt " +
                "-verify_hostname my-cluster-zookeeper-client";
        String outputForZookeeperClient =
                kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, 0), "kafka", "/bin/bash", "-c", commandForZookeeperClient).out();
        checkZookeeperCertificates(outputForZookeeperClient);

        IntStream.rangeClosed(0, 1).forEach(podId -> {
            String commandForKafkaPort9091 = generateOpenSSLCommandWithCerts(kafkaPodName(CLUSTER_NAME, podId), "my-cluster-kafka-brokers", "9091");
            String commandForKafkaPort9093 = generateOpenSSLCommandWithCAfile(kafkaPodName(CLUSTER_NAME, podId), "my-cluster-kafka-brokers", "9093");

            String outputForKafkaPort9091 =
                    kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c", commandForKafkaPort9091).out();
            String outputForKafkaPort9093 =
                    kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c", commandForKafkaPort9093).out();

            checkKafkaCertificates(outputForKafkaPort9091, outputForKafkaPort9093);

            String commandForZookeeperPort2181 = generateOpenSSLCommandWithCAfile(zookeeperPodName(CLUSTER_NAME, podId), "my-cluster-zookeeper-nodes", "2181");
            String commandForZookeeperPort2888 = generateOpenSSLCommandWithCerts(zookeeperPodName(CLUSTER_NAME, podId), "my-cluster-zookeeper-nodes", "2888");
            String commandForZookeeperPort3888 = generateOpenSSLCommandWithCerts(zookeeperPodName(CLUSTER_NAME, podId), "my-cluster-zookeeper-nodes", "3888");

            String outputForZookeeperPort2181 =
                    kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c", commandForZookeeperPort2181).out();
            String outputForZookeeperPort3888 =
                    kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c", commandForZookeeperPort3888).out();
            checkZookeeperCertificates(outputForZookeeperPort2181, outputForZookeeperPort3888);

            try {
                String outputForZookeeperPort2888 =
                        kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c", commandForZookeeperPort2888).out();
                checkZookeeperCertificates(outputForZookeeperPort2888);
            } catch (KubeClusterException e) {
                if (e.result != null && e.result.exitStatus() == 104) {
                    LOGGER.info("The connection for {} was forcibly closed because of new zookeeper leader", zookeeperPodName(CLUSTER_NAME, podId));
                } else {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private String generateOpenSSLCommandWithCAfile(String podName, String hostname, String port) {
        return "openssl s_client -connect " + podName + "." + hostname + ":" + port +
                " -showcerts -CAfile /opt/kafka/broker-certs/cluster-ca.crt " +
                "-verify_hostname " + podName + "." + hostname + "." + NAMESPACE + ".svc.cluster.local";
    }

    private String generateOpenSSLCommandWithCerts(String podName, String hostname, String port) {
        return "openssl s_client -connect " + podName + "." + hostname + ":" + port + " -showcerts -CAfile /opt/kafka/broker-certs/cluster-ca.crt " +
                "-cert /opt/kafka/broker-certs/my-cluster-kafka-0.crt " +
                "-key /opt/kafka/broker-certs/my-cluster-kafka-0.key " +
                "-verify_hostname " + podName + "." + hostname + "." + NAMESPACE + ".svc.cluster.local";
    }

    private void checkKafkaCertificates(String... kafkaCertificates) {
        String kafkaCertificateChain = "s:/O=io.strimzi/CN=my-cluster-kafka\n" +
                "   i:/O=io.strimzi/CN=cluster-ca";
        String kafkaBrokerCertificate = "Server certificate\n" +
                "subject=/O=io.strimzi/CN=my-cluster-kafka\n" +
                "issuer=/O=io.strimzi/CN=cluster-ca";
        for (String kafkaCertificate : kafkaCertificates) {
            Assert.assertThat(kafkaCertificate, containsString(kafkaCertificateChain));
            Assert.assertThat(kafkaCertificate, containsString(kafkaBrokerCertificate));
            Assert.assertThat(kafkaCertificate, containsString(TLS_PROTOCOL));
            Assert.assertThat(kafkaCertificate, containsString(SSL_TIMEOUT));
            Assert.assertThat(kafkaCertificate, containsString(OPENSSL_RETURN_CODE));
        }
    }

    private void checkZookeeperCertificates(String... zookeeperCertificates) {
        String zookeeperCertificateChain = "s:/O=io.strimzi/CN=my-cluster-zookeeper\n" +
                "   i:/O=io.strimzi/CN=cluster-ca";
        String zookeeperNodeCertificate = "Server certificate\n" +
                "subject=/O=io.strimzi/CN=my-cluster-zookeeper\n" +
                "issuer=/O=io.strimzi/CN=cluster-ca";
        for (String zookeeperCertificate : zookeeperCertificates) {
            Assert.assertThat(zookeeperCertificate, containsString(zookeeperCertificateChain));
            Assert.assertThat(zookeeperCertificate, containsString(zookeeperNodeCertificate));
            Assert.assertThat(zookeeperCertificate, containsString(TLS_PROTOCOL));
            Assert.assertThat(zookeeperCertificate, containsString(SSL_TIMEOUT));
            Assert.assertThat(zookeeperCertificate, containsString(OPENSSL_RETURN_CODE));
        }
    }
}
