/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class CruiseControlUtils {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlUtils.class);

    public static final String CRUISE_CONTROL_METRICS_TOPIC = "strimzi.cruisecontrol.metrics"; // partitions 1 , rf - 1
    public static final String CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC = "strimzi.cruisecontrol.modeltrainingsamples"; // partitions 32 , rf - 2
    public static final String CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC = "strimzi.cruisecontrol.partitionmetricsamples"; // partitions 32 , rf - 2

    private static final int CRUISE_CONTROL_DEFAULT_PORT = 9090;
    private static final int CRUISE_CONTROL_METRICS_PORT = 9404;

    private static final String CONTAINER_NAME = "cruise-control";

    private CruiseControlUtils() { }

    public enum SupportedHttpMethods {
        GET,
        POST
    }

    public enum SupportedSchemes {
        HTTP,
        HTTPS
    }

    public static String callApi(String namespaceName, SupportedHttpMethods method, CruiseControlEndpoints endpoint, SupportedSchemes scheme, Boolean withCredentials) {
        return callApi(namespaceName, method, endpoint, scheme, withCredentials, "");
    }

    @SuppressWarnings("Regexp")
    @SuppressFBWarnings("DM_CONVERT_CASE")
    public static String callApi(String namespaceName, SupportedHttpMethods method, CruiseControlEndpoints endpoint, SupportedSchemes scheme, Boolean withCredentials, String endpointSuffix) {
        String ccPodName = PodUtils.getFirstPodNameContaining(namespaceName, CONTAINER_NAME);
        String args = " -k ";

        if (withCredentials) {
            args = " --cacert /etc/cruise-control/cc-certs/cruise-control.crt"
                + " --user admin:$(cat /opt/cruise-control/api-auth-config/cruise-control.apiAdminPassword) ";
        }

        return cmdKubeClient(namespaceName).execInPodContainer(Level.DEBUG, ccPodName, CONTAINER_NAME, "/bin/bash", "-c",
            "curl -X " + method.name() + args + " " + scheme + "://localhost:" + CRUISE_CONTROL_DEFAULT_PORT + endpoint.toString() + endpointSuffix).out();
    }

    @SuppressWarnings("Regexp")
    @SuppressFBWarnings("DM_CONVERT_CASE")
    public static String callApi(String namespaceName, SupportedHttpMethods method, String endpoint) {
        String ccPodName = PodUtils.getFirstPodNameContaining(namespaceName, CONTAINER_NAME);

        return cmdKubeClient(namespaceName).execInPodContainer(Level.DEBUG, ccPodName, CONTAINER_NAME, "/bin/bash", "-c",
            "curl -X" + method.name() + " localhost:" + CRUISE_CONTROL_METRICS_PORT + endpoint).out();
    }

    @SuppressWarnings("BooleanExpressionComplexity")
    public static void verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(Properties kafkaProperties) {
        String kafkaClusterName = kafkaProperties.getProperty("cluster-name");
        TestUtils.waitFor("Verify that Kafka configuration " + kafkaProperties.toString() + " has correct CruiseControl metric reporter properties",
            TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_CRUISE_CONTROL_TIMEOUT, () ->
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_TOPIC_NAME.getValue()).equals("strimzi.cruisecontrol.metrics") &&
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_ENDPOINT_ID_ALGO.getValue()).equals("HTTPS") &&
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_BOOTSTRAP_SERVERS.getValue()).equals(kafkaClusterName + "-kafka-brokers:9091") &&
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SECURITY_PROTOCOL.getValue()).equals("SSL") &&
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_TYPE.getValue()).equals("PKCS12") &&
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_LOCATION.getValue()).equals("/tmp/kafka/cluster.keystore.p12") &&
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_PASSWORD.getValue()).equals("${CERTS_STORE_PASSWORD}") &&
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_TYPE.getValue()).equals("PKCS12") &&
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_LOCATION.getValue()).equals("/tmp/kafka/cluster.truststore.p12") &&
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_PASSWORD.getValue()).equals("${CERTS_STORE_PASSWORD}"));
    }

    public static void verifyThatCruiseControlSamplesTopicsArePresent(String namespaceName, long timeout) {
        final int numberOfPartitionsSamplesTopic = 32;
        final int numberOfReplicasSamplesTopic = 2;

        TestUtils.waitFor("Verify that Kafka contains CruiseControl Topics with related configuration.",
            TestConstants.GLOBAL_POLL_INTERVAL, timeout, () -> {
                KafkaTopic modelTrainingSamples = KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC).get();
                KafkaTopic partitionsMetricsSamples = KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC).get();

                if (modelTrainingSamples != null && partitionsMetricsSamples != null) {
                    boolean hasTopicCorrectPartitionsCount =
                            modelTrainingSamples.getSpec().getPartitions() == numberOfPartitionsSamplesTopic &&
                            partitionsMetricsSamples.getSpec().getPartitions() == numberOfPartitionsSamplesTopic;

                    boolean hasTopicCorrectReplicasCount =
                            modelTrainingSamples.getSpec().getReplicas() == numberOfReplicasSamplesTopic &&
                            partitionsMetricsSamples.getSpec().getReplicas() == numberOfReplicasSamplesTopic;

                    return hasTopicCorrectPartitionsCount && hasTopicCorrectReplicasCount;
                }
                LOGGER.debug("One of the samples {}, {} Topics are not present", CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC, CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);
                return false;
            });
    }

    public static void verifyThatKafkaCruiseControlMetricReporterTopicIsPresent(String namespaceName, long timeout) {
        final int numberOfPartitionsMetricTopic = 1;
        final int numberOfReplicasMetricTopic = 3;

        TestUtils.waitFor("Verify that Kafka contains CruiseControl topics with related configuration.",
            TestConstants.GLOBAL_POLL_INTERVAL, timeout, () -> {
                KafkaTopic metrics = KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(CRUISE_CONTROL_METRICS_TOPIC).get();

                boolean hasTopicCorrectPartitionsCount =
                    metrics.getSpec().getPartitions() == numberOfPartitionsMetricTopic;

                boolean hasTopicCorrectReplicasCount =
                    metrics.getSpec().getReplicas() == numberOfReplicasMetricTopic;

                return hasTopicCorrectPartitionsCount && hasTopicCorrectReplicasCount;
            });
    }

    public static void verifyThatCruiseControlTopicsArePresent(String namespaceName) {
        verifyThatKafkaCruiseControlMetricReporterTopicIsPresent(namespaceName, TestConstants.GLOBAL_CRUISE_CONTROL_TIMEOUT);
        verifyThatCruiseControlSamplesTopicsArePresent(namespaceName, TestConstants.GLOBAL_CRUISE_CONTROL_TIMEOUT);
    }

    public static Properties getKafkaCruiseControlMetricsReporterConfiguration(String namespaceName, String clusterName) throws IOException {
        String cmName = KafkaResource.getKafkaPodName(clusterName, 0);

        InputStream configurationFileStream = new ByteArrayInputStream(kubeClient(namespaceName).getConfigMap(namespaceName, cmName)
                .getData().get("server.config").getBytes(StandardCharsets.UTF_8));

        Properties configurationOfKafka = new Properties();
        configurationOfKafka.load(configurationFileStream);

        Properties cruiseControlProperties = new Properties();

        for (Map.Entry<Object, Object> entry : configurationOfKafka.entrySet()) {
            if (entry.getKey().toString().startsWith("cruise.control.metrics")) {
                cruiseControlProperties.put(entry.getKey(), entry.getValue());
            }
        }
        cruiseControlProperties.put("cluster-name", clusterName);

        return cruiseControlProperties;
    }

    public static void waitForRebalanceEndpointIsReady(String namespaceName) {
        TestUtils.waitFor("rebalance endpoint to be ready",
            TestConstants.API_CRUISE_CONTROL_POLL, TestConstants.API_CRUISE_CONTROL_TIMEOUT, () -> {
                String response = callApi(namespaceName, SupportedHttpMethods.POST, CruiseControlEndpoints.REBALANCE, SupportedSchemes.HTTPS, true);
                LOGGER.debug("API response: {}", response);
                return !response.contains("Error processing POST request '/rebalance' due to: " +
                    "'com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException: " +
                    "com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException: ");
            });
    }

    /**
     * Returns user defined network capacity value without KiB/s suffix.
     *
     * @param userCapacity User defined network capacity with KiB/s suffix.
     *
     * @return User defined network capacity without KiB/s as a Double.
     */
    public static Double removeNetworkCapacityKibSuffix(String userCapacity) {
        return Double.valueOf(userCapacity.substring(0, userCapacity.length() - 5));
    }
}
