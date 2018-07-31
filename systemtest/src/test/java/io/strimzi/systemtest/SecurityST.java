/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.ClusterOperator;
import io.strimzi.test.JUnitGroup;
import io.strimzi.test.KafkaFromClasspathYaml;
import io.strimzi.test.Namespace;
import io.strimzi.test.StrimziRunner;
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

    private static final Logger LOGGER = LogManager.getLogger(SecurityST.class);
    public static final String NAMESPACE = "security-cluster-test";
    private String OPENSSL_RETURN_CODE = "Verify return code: 0 (ok)";
    private String TLS_PROTOCOL = "Protocol  : TLSv1";
    private String SSL_TIMEOUT = "Timeout   : 300 (sec)";

    @Test
    @JUnitGroup(name = "regression")
    @KafkaFromClasspathYaml()
    public void testCertificates() {
        LOGGER.info("Running testCertificates {}", CLUSTER_NAME);

        // Testing Kafka ports by hostname
        String commandForKafkaPort9091 = generateOpenSSLCommandWithCerts("my-cluster-kafka-bootstrap:9091");
        String commandForKafkaPort9093 = generateOpenSSLCommandWithCAfile("my-cluster-kafka-bootstrap:9093");

        String outputForKafkaPort9091 =
                kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, 0), "kafka", "/bin/bash", "-c", commandForKafkaPort9091).out();
        String outputForKafkaPort9093 =
                kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, 0), "kafka", "/bin/bash", "-c", commandForKafkaPort9093).out();

        checkKafkaCertificates(new String[]{outputForKafkaPort9091, outputForKafkaPort9093});

        // Testing Zookeeper ports by hostname
        String commandForZookeeperPort2181 = generateOpenSSLCommandWithCAfile("my-cluster-zookeeper-client:2181");
        String commandForZookeeperPort2888 = generateOpenSSLCommandWithCerts("my-cluster-zookeeper-nodes:2888");
        String commandForZookeeperPort3888 = generateOpenSSLCommandWithCerts("my-cluster-zookeeper-nodes:3888");

        String outputForZookeeperPort2181 =
                kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, 0), "kafka", "/bin/bash", "-c", commandForZookeeperPort2181).out();

        String outputForZookeeperPort2888 =
                kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, 0), "kafka", "/bin/bash", "-c", commandForZookeeperPort2888).out();

        String outputForZookeeperPort3888 =
                kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, 0), "kafka", "/bin/bash", "-c", commandForZookeeperPort3888).out();

        checkZookeeperCertificates(new String[]{outputForZookeeperPort2181, outputForZookeeperPort2888, outputForZookeeperPort3888});

        IntStream.rangeClosed(0, 1).forEach(podId -> {
            //Testing Kafka ports by IP address
            String kafkaIPAddress = getIPAddressFromContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka");

            String commandForKafkaPort9091ByIP = generateOpenSSLCommandWithCerts(kafkaIPAddress + ":9091");
            String commandForKafkaPort9093ByIP = generateOpenSSLCommandWithCAfile(kafkaIPAddress + ":9093");

            String outputForKafkaPort9091ByIP =
                    kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c", commandForKafkaPort9091ByIP).out();
            String outputForKafkaPort9093ByIP =
                    kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c", commandForKafkaPort9093ByIP).out();

            checkKafkaCertificates(new String[]{outputForKafkaPort9091ByIP, outputForKafkaPort9093ByIP});

            //Testing Zookeeper ports by IP address
            String zookeeperIPAddress = getIPAddressFromContainer(zookeeperPodName(CLUSTER_NAME, podId), "zookeeper");

            String commandForZookeeperPort2181ByIP = generateOpenSSLCommandWithCAfile(zookeeperIPAddress + ":2181");
            String commandForZookeeperPort2888ByIP = generateOpenSSLCommandWithCerts(zookeeperIPAddress + ":2888");
            String commandForZookeeperPort3888ByIP = generateOpenSSLCommandWithCerts(zookeeperIPAddress + ":3888");

            String outputForZookeeperPort2181ByIP =
                    kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c", commandForZookeeperPort2181ByIP).out();

            String outputForZookeeperPort2888ByIP =
                    kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c", commandForZookeeperPort2888ByIP).out();

            String outputForZookeeperPort3888ByIP =
                    kubeClient.execInPodContainer(kafkaPodName(CLUSTER_NAME, podId), "kafka", "/bin/bash", "-c", commandForZookeeperPort3888ByIP).out();

            checkZookeeperCertificates(new String[]{outputForZookeeperPort2181ByIP, outputForZookeeperPort2888ByIP, outputForZookeeperPort3888ByIP});
        });

    }

    private String getIPAddressFromContainer(String podName, String containerName) {
        return kubeClient.execInPodContainer(podName, containerName, "/bin/bash", "-c", "hostname -I").out().trim();
    }

    private String generateOpenSSLCommandWithCAfile(String hostname) {
        return "openssl s_client -connect " + hostname + " -showcerts " +
                "-CAfile /opt/kafka/broker-certs/cluster-ca.crt";
    }

    private String generateOpenSSLCommandWithCerts(String hostname) {
        return "openssl s_client -connect " + hostname + " -showcerts " +
                "-CAfile /opt/kafka/broker-certs/cluster-ca.crt " +
                "-cert /opt/kafka/broker-certs/my-cluster-kafka-0.crt " +
                "-key /opt/kafka/broker-certs/my-cluster-kafka-0.key";
    }

    private void checkKafkaCertificates(String[] kafkaCertificates) {
        String kafkaCertificateChain = "s:/O=io.strimzi/CN=my-cluster-kafka\n" +
                "   i:/O=io.strimzi/CN=cluster-ca";

        String kafkaBrokerCertificate = "Server certificate\n" +
                "subject=/O=io.strimzi/CN=my-cluster-kafka\n" +
                "issuer=/O=io.strimzi/CN=cluster-ca";

        for (String certificate : kafkaCertificates) {
            Assert.assertThat(certificate, containsString(kafkaCertificateChain));
            Assert.assertThat(certificate, containsString(kafkaBrokerCertificate));
            Assert.assertThat(certificate, containsString(TLS_PROTOCOL));
            Assert.assertThat(certificate, containsString(SSL_TIMEOUT));
            Assert.assertThat(certificate, containsString(OPENSSL_RETURN_CODE));
        }
    }

    private void checkZookeeperCertificates(String[] zookeeperCertificates) {
        String zookeeperCertificateChain = "s:/O=io.strimzi/CN=my-cluster-zookeeper\n" +
                "   i:/O=io.strimzi/CN=cluster-ca";

        String zookeeperNodeCertificate = "Server certificate\n" +
                "subject=/O=io.strimzi/CN=my-cluster-zookeeper\n" +
                "issuer=/O=io.strimzi/CN=cluster-ca";

        for (String certificate : zookeeperCertificates) {
            Assert.assertThat(certificate, containsString(zookeeperCertificateChain));
            Assert.assertThat(certificate, containsString(zookeeperNodeCertificate));
            Assert.assertThat(certificate, containsString(TLS_PROTOCOL));
            Assert.assertThat(certificate, containsString(SSL_TIMEOUT));
            Assert.assertThat(certificate, containsString(OPENSSL_RETURN_CODE));
        }
    }
}
