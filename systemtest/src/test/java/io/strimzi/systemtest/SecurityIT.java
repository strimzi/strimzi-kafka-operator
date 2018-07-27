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
@Namespace(SecurityIT.NAMESPACE)
@ClusterOperator
public class SecurityIT extends AbstractIT {

    private static final Logger LOGGER = LogManager.getLogger(SecurityIT.class);
    public static final String NAMESPACE = "security-cluster-test";

    @Test
    @JUnitGroup(name = "regression")
    @KafkaFromClasspathYaml()
    public void testCertificates() {
        LOGGER.info("Running testCertificates {}", CLUSTER_NAME);

        String kafkaCertificateChain = "s:/O=io.strimzi/CN=my-cluster-kafka\n" +
                "   i:/O=io.strimzi/CN=cluster-ca";

        String kafkaServerCertificate = "Server certificate\n" +
                "subject=/O=io.strimzi/CN=my-cluster-kafka\n" +
                "issuer=/O=io.strimzi/CN=cluster-ca";

        String zookeeperCertificateChain = "s:/O=io.strimzi/CN=my-cluster-zookeeper\n" +
                "   i:/O=io.strimzi/CN=cluster-ca";

        String zookeeperServerCertificate = "Server certificate\n" +
                "subject=/O=io.strimzi/CN=my-cluster-zookeeper\n" +
                "issuer=/O=io.strimzi/CN=cluster-ca";

        String protocol = "Protocol  : TLSv1.2";
        String timeout = "Timeout   : 300 (sec)";

        IntStream.rangeClosed(0, 1).forEach(podId -> {
            String kafkaCertificateInfo = getKafkaCertificate(podId);
            String zookeeperCertificateInfo = getZookeeperCertificate(podId);

            Assert.assertThat(kafkaCertificateInfo, containsString(kafkaCertificateChain));
            Assert.assertThat(kafkaCertificateInfo, containsString(kafkaServerCertificate));
            Assert.assertThat(kafkaCertificateInfo, containsString(protocol));
            Assert.assertThat(kafkaCertificateInfo, containsString(timeout));

            Assert.assertThat(zookeeperCertificateInfo, containsString(zookeeperCertificateChain));
            Assert.assertThat(zookeeperCertificateInfo, containsString(zookeeperServerCertificate));
            Assert.assertThat(zookeeperCertificateInfo, containsString(protocol));
            Assert.assertThat(zookeeperCertificateInfo, containsString(timeout));
        });
    }

    private String getKafkaCertificate(int podIndex) {
        String kafkaPodName = kafkaPodName(CLUSTER_NAME, podIndex);
        return kubeClient.execInPod(kafkaPodName, "/bin/bash", "-c", "openssl s_client -connect localhost:9093 -showcerts").out();
    }

    private String getZookeeperCertificate(int podIndex) {
        String kafkaPodName = zookeeperPodName(CLUSTER_NAME, podIndex);
        return kubeClient.execInPod(kafkaPodName, "/bin/bash", "-c", "openssl s_client -connect localhost:2181 -showcerts").out();
    }
}
