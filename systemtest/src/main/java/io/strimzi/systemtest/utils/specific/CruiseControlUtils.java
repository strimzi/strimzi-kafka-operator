/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.skodjob.testframe.enums.LogLevel;
import io.skodjob.testframe.executor.ExecResult;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.KafkaTopicDescription;
import io.strimzi.systemtest.labels.LabelSelectors;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.utils.AdminClientUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
        private final String responseText;
        private final int responseCode;

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

    public static ApiResult callApiWithAdminCredentials(String namespaceName, HttpMethod method, Scheme scheme, int port, String endpoint, String endpointParameters) {
        return callApi(
            namespaceName,
            method,
            scheme,
            port,
            endpoint,
            endpointParameters,
            "admin:$(cat /opt/cruise-control/api-auth-config/cruise-control.apiAdminPassword)"
        );
    }

    public static ApiResult callApi(String namespaceName, HttpMethod method, Scheme scheme, int port, String endpoint, String endpointParameters) {
        return callApi(
            namespaceName,
            method,
            scheme,
            port,
            endpoint,
            endpointParameters,
            ""
        );
    }

    @SuppressFBWarnings("DM_CONVERT_CASE")
    public static ApiResult callApi(String namespaceName, HttpMethod method, Scheme scheme, int port, String endpoint, String endpointParameters, String userCreds) {
        String ccPodName = PodUtils.getFirstPodNameContaining(namespaceName, CONTAINER_NAME);
        String args = " -k -w \"%{http_code}\" ";

        if (!userCreds.isEmpty()) {
            args += String.format(" --user %s ", userCreds);
        }

        String curl = "curl -X " + method.name() + " " + args + " " + scheme + "://localhost:" + port + endpoint + endpointParameters;
        return new ApiResult(KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPodContainer(LogLevel.DEBUG, ccPodName, CONTAINER_NAME, "/bin/bash", "-c", curl));
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
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_PASSWORD.getValue()).equals("${strimzienv:CERTS_STORE_PASSWORD}") &&
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_TYPE.getValue()).equals("PKCS12") &&
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_LOCATION.getValue()).equals("/tmp/kafka/cluster.truststore.p12") &&
            kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_PASSWORD.getValue()).equals("${strimzienv:CERTS_STORE_PASSWORD}"));
    }

    public static void verifyThatCruiseControlTopicsArePresent(AdminClient adminClient, int defaultReplicaCount) {

        LOGGER.info("Waiting for Cruise Control topics to be present in Kafka");

        AdminClientUtils.waitForTopicPresence(adminClient, CRUISE_CONTROL_METRICS_TOPIC);
        AdminClientUtils.waitForTopicPresence(adminClient, CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        AdminClientUtils.waitForTopicPresence(adminClient, CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);

        KafkaTopicDescription ccMetricTopic = adminClient.describeTopic(CRUISE_CONTROL_METRICS_TOPIC);
        KafkaTopicDescription ccModelTrainingTopic = adminClient.describeTopic(CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        KafkaTopicDescription ccPartitionMetricTopic = adminClient.describeTopic(CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);

        LOGGER.info("Verifying Cruise Control topics have expected replications");
        assertThat(ccMetricTopic.replicaCount(), is(defaultReplicaCount));
        assertThat(ccModelTrainingTopic.replicaCount(), is(defaultReplicaCount));
        assertThat(ccPartitionMetricTopic.replicaCount(), is(defaultReplicaCount));

        LOGGER.info("Verifying Cruise Control topics have expected partitions");
        assertThat(ccMetricTopic.partitionCount(), is(1));
        assertThat(ccModelTrainingTopic.partitionCount(), is(32));
        assertThat(ccPartitionMetricTopic.partitionCount(), is(32));
    }

    public static Properties getKafkaCruiseControlMetricsReporterConfiguration(String namespaceName, String clusterName) throws IOException {
        String cmName = KubeResourceManager.get().kubeClient().listPods(namespaceName, LabelSelectors.kafkaLabelSelector(clusterName, KafkaComponents.getBrokerPodSetName(clusterName))).get(0).getMetadata().getName();

        InputStream configurationFileStream = new ByteArrayInputStream(KubeResourceManager.get().kubeClient().getClient().configMaps().inNamespace(namespaceName).withName(cmName).get()
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
}
