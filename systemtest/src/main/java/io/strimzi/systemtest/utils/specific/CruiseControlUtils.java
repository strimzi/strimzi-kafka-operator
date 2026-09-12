/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.skodjob.kubetest4j.enums.LogLevel;
import io.skodjob.kubetest4j.executor.ExecResult;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityEncryptionType;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.KafkaTopicDescription;
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
    public static void verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(String clusterName, String namespace, String brokerPodName) throws IOException {
        Properties kafkaProperties = getKafkaCruiseControlMetricsReporterConfiguration(namespace, clusterName, brokerPodName);
        String kafkaClusterName = kafkaProperties.getProperty("cluster-name");
        TestUtils.waitFor("Verify that Kafka configuration " + kafkaProperties + " has correct CruiseControl metric reporter properties",
            TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_CRUISE_CONTROL_TIMEOUT, () ->
                        kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_TOPIC_NAME.getValue()).equals("strimzi.cruisecontrol.metrics")
                                && kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_TOPIC_AUTO_CREATE.getValue()).equals("true")
                                && kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_BOOTSTRAP_SERVERS.getValue()).equals(kafkaClusterName + "-kafka-brokers:9091")
                                && verifyCruiseControlMetricReporterConfiguration(kafkaProperties, clusterName, namespace, brokerPodName));
    }

    /**
     * Verifies that the Cruise Control metrics reporter is configured according to the internal cluster security
     * configuration used for the whole test run.
     *
     * @param kafkaProperties   Kafka broker configuration with the Cruise Control metrics reporter options
     * @param clusterName       Name of the Kafka cluster
     * @param namespace         Namespace of the Kafka cluster
     * @param brokerPodName     Name of the broker pod the configuration belongs to
     *
     * @return  True when the configuration matches the expected internal cluster security configuration. False otherwise.
     */
    private static boolean verifyCruiseControlMetricReporterConfiguration(Properties kafkaProperties, String clusterName, String namespace, String brokerPodName)  {
        return verifyCruiseControlMetricReporterSecurityProtocol(kafkaProperties)
                && verifyCruiseControlMetricReporterEncryption(kafkaProperties, clusterName, namespace)
                && verifyCruiseControlMetricReporterAuthentication(kafkaProperties, namespace, brokerPodName);
    }

    /**
     * Verifies the security protocol of the Cruise Control metrics reporter. It is always configured and depends on
     * both the encryption and the authentication type.
     *
     * @param kafkaProperties   Kafka broker configuration with the Cruise Control metrics reporter options
     *
     * @return  True when the security protocol matches the expected one. False otherwise.
     */
    private static boolean verifyCruiseControlMetricReporterSecurityProtocol(Properties kafkaProperties) {
        String expected = ClusterSecurityEncryptionType.TLS.equals(Environment.CLUSTER_SECURITY_ENCRYPTION) ? "SSL" : "PLAINTEXT";

        if (ClusterSecurityAuthenticationType.SERVICE_ACCOUNT.equals(Environment.CLUSTER_SECURITY_AUTHENTICATION)) {
            expected = "SASL_" + expected;
        }

        return expected.equals(kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SECURITY_PROTOCOL.getValue()));
    }

    /**
     * Verifies the encryption configuration of the Cruise Control metrics reporter. The truststore with the Cluster CA
     * is configured only when TLS encryption is used.
     *
     * @param kafkaProperties   Kafka broker configuration with the Cruise Control metrics reporter options
     * @param clusterName       Name of the Kafka cluster
     * @param namespace         Namespace of the Kafka cluster
     *
     * @return  True when the encryption configuration matches the expected one. False otherwise.
     */
    private static boolean verifyCruiseControlMetricReporterEncryption(Properties kafkaProperties, String clusterName, String namespace) {
        if (ClusterSecurityEncryptionType.TLS.equals(Environment.CLUSTER_SECURITY_ENCRYPTION)) {
            return "HTTPS".equals(kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_ENDPOINT_ID_ALGO.getValue()))
                    && "PEM".equals(kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_TYPE.getValue()))
                    && ("${strimzisecrets:" + namespace + "/" + clusterName + "-trustbundle:cluster-ca.crt}").equals(kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_CERTIFICATES.getValue()));
        } else {
            return !kafkaProperties.containsKey(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_ENDPOINT_ID_ALGO.getValue())
                    && !kafkaProperties.containsKey(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_TYPE.getValue())
                    && !kafkaProperties.containsKey(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_CERTIFICATES.getValue());
        }
    }

    /**
     * Verifies the authentication configuration of the Cruise Control metrics reporter. mTLS authentication uses the
     * keystore with the broker certificate while Service Account authentication uses SASL OAUTHBEARER with the
     * projected Service Account token.
     *
     * @param kafkaProperties   Kafka broker configuration with the Cruise Control metrics reporter options
     * @param namespace         Namespace of the Kafka cluster
     * @param brokerPodName     Name of the broker pod the configuration belongs to
     *
     * @return  True when the authentication configuration matches the expected one. False otherwise.
     */
    private static boolean verifyCruiseControlMetricReporterAuthentication(Properties kafkaProperties, String namespace, String brokerPodName) {
        boolean noKeystore = !kafkaProperties.containsKey(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_TYPE.getValue())
                && !kafkaProperties.containsKey(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_CERTIFICATE_CHAIN.getValue())
                && !kafkaProperties.containsKey(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_KEY.getValue());
        boolean noSasl = !kafkaProperties.containsKey("cruise.control.metrics.reporter.sasl.mechanism")
                && !kafkaProperties.containsKey("cruise.control.metrics.reporter.sasl.login.callback.handler.class")
                && !kafkaProperties.containsKey("cruise.control.metrics.reporter.sasl.jaas.config");

        return switch (Environment.CLUSTER_SECURITY_AUTHENTICATION) {
            case MTLS -> "PEM".equals(kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_TYPE.getValue()))
                    && ("${strimzisecrets:" + namespace + "/" + brokerPodName + ":" + brokerPodName + ".crt}").equals(kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_CERTIFICATE_CHAIN.getValue()))
                    && ("${strimzisecrets:" + namespace + "/" + brokerPodName + ":" + brokerPodName + ".key}").equals(kafkaProperties.getProperty(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_KEY.getValue()))
                    && noSasl;
            case SERVICE_ACCOUNT -> "OAUTHBEARER".equals(kafkaProperties.getProperty("cruise.control.metrics.reporter.sasl.mechanism"))
                    && kafkaProperties.containsKey("cruise.control.metrics.reporter.sasl.login.callback.handler.class")
                    && kafkaProperties.getProperty("cruise.control.metrics.reporter.sasl.jaas.config", "").contains("/var/run/secrets/strimzi.io/token")
                    && noKeystore;
            case NONE -> noKeystore && noSasl;
        };
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

    public static Properties getKafkaCruiseControlMetricsReporterConfiguration(String namespaceName, String clusterName, String cmName) throws IOException {
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
