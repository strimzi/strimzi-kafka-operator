/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.ExecResult;
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

    public static final int CRUISE_CONTROL_DEFAULT_PORT = 9090;
    public static final int CRUISE_CONTROL_METRICS_PORT = 9404;

    private static final String CONTAINER_NAME = "cruise-control";

    public enum HttpMethod {
        GET,
        POST
    }

    public enum Scheme {
        HTTP,
        HTTPS
    }

    public static class ApiResult {
        private String responseText;
        private int responseCode;

        public ApiResult(ExecResult execResult) {
            this.responseText = execResult.out();
            this.responseCode = responseCode(execResult.out());
        }

        private int responseCode(String responseText) {
            responseText = responseText.replaceAll("\n", "");
            return Integer.parseInt(responseText.substring(responseText.length() - 3));
        }

        public String getResponseText() {
            return responseText;
        }

        public int getResponseCode() {
            return responseCode;
        }
    }

    @SuppressFBWarnings("DM_CONVERT_CASE")
    public static ApiResult callApi(String namespaceName, HttpMethod method, Scheme scheme, int port, String endpoint, String endpointParameters, boolean withCredentials) {
        String ccPodName = PodUtils.getFirstPodNameContaining(namespaceName, CONTAINER_NAME);
        String args = " -k -w \"%{http_code}\" ";

        if (withCredentials) {
            args += " --user admin:$(cat /opt/cruise-control/api-auth-config/cruise-control.apiAdminPassword) ";
        }

        String curl = "curl -X " + method.name() + " " + args + " " + scheme + "://localhost:" + port + endpoint + endpointParameters;
        return new ApiResult(cmdKubeClient(namespaceName).execInPodContainer(Level.DEBUG, ccPodName, CONTAINER_NAME, "/bin/bash", "-c", curl));
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

    public static void verifyThatCruiseControlSamplesTopicsArePresent(String namespaceName, int defaultReplicaCount, long timeout) {
        final int numberOfPartitionsSamplesTopic = 32;

        TestUtils.waitFor("Verify that Kafka contains CruiseControl Topics with related configuration.",
            TestConstants.GLOBAL_POLL_INTERVAL, timeout, () -> {
                KafkaTopic modelTrainingSamples = KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC).get();
                KafkaTopic partitionsMetricsSamples = KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC).get();

                if (modelTrainingSamples != null && partitionsMetricsSamples != null) {
                    boolean hasTopicCorrectPartitionsCount =
                            modelTrainingSamples.getSpec().getPartitions() == numberOfPartitionsSamplesTopic &&
                            partitionsMetricsSamples.getSpec().getPartitions() == numberOfPartitionsSamplesTopic;

                    boolean hasTopicCorrectReplicasCount =
                            modelTrainingSamples.getSpec().getReplicas() == defaultReplicaCount &&
                            partitionsMetricsSamples.getSpec().getReplicas() == defaultReplicaCount;

                    return hasTopicCorrectPartitionsCount && hasTopicCorrectReplicasCount;
                }
                LOGGER.debug("One of the samples {}, {} Topics are not present", CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC, CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);
                return false;
            });
    }

    public static void verifyThatKafkaCruiseControlMetricReporterTopicIsPresent(String namespaceName, int defaultReplicaCount, long timeout) {
        final int numberOfPartitionsMetricTopic = 1;

        TestUtils.waitFor("Verify that Kafka contains CruiseControl topics with related configuration.",
            TestConstants.GLOBAL_POLL_INTERVAL, timeout, () -> {
                KafkaTopic metrics = KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(CRUISE_CONTROL_METRICS_TOPIC).get();

                boolean hasTopicCorrectPartitionsCount =
                    metrics.getSpec().getPartitions() == numberOfPartitionsMetricTopic;

                boolean hasTopicCorrectReplicasCount =
                    metrics.getSpec().getReplicas() == defaultReplicaCount;

                return hasTopicCorrectPartitionsCount && hasTopicCorrectReplicasCount;
            });
    }

    public static void verifyThatCruiseControlTopicsArePresent(String namespaceName, int defaultReplicaCount) {
        verifyThatKafkaCruiseControlMetricReporterTopicIsPresent(namespaceName, defaultReplicaCount, TestConstants.GLOBAL_CRUISE_CONTROL_TIMEOUT);
        verifyThatCruiseControlSamplesTopicsArePresent(namespaceName, defaultReplicaCount, TestConstants.GLOBAL_CRUISE_CONTROL_TIMEOUT);
    }

    public static Properties getKafkaCruiseControlMetricsReporterConfiguration(String namespaceName, String clusterName) throws IOException {
        String cmName = kubeClient().listPods(namespaceName, KafkaResource.getLabelSelector(clusterName, StrimziPodSetResource.getBrokerComponentName(clusterName))).get(0).getMetadata().getName();

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
