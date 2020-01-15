/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectS2IUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.CoreMatchers.is;

@OpenShiftOnly
@Tag(REGRESSION)
class ConnectS2IST extends MessagingBaseST {

    public static final String NAMESPACE = "connect-s2i-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(ConnectS2IST.class);
    private static final String CONNECT_S2I_TOPIC_NAME = "connect-s2i-topic-example";

    @Test
    void testDeployS2IWithMongoDBPlugin() throws InterruptedException {
        final String kafkaConnectS2IName = "kafka-connect-s2i-name-1";
        // Calls to Connect API are executed from kafka-0 pod
        String podForExecName = deployConnectS2IWithMongoDb(kafkaConnectS2IName);

        String mongoDbConfig = "{" +
                "\"name\": \"" + kafkaConnectS2IName + "\"," +
                "\"config\": {" +
                "   \"connector.class\" : \"io.debezium.connector.mongodb.MongoDbConnector\"," +
                "   \"tasks.max\" : \"1\"," +
                "   \"mongodb.hosts\" : \"debezium/localhost:27017\"," +
                "   \"mongodb.name\" : \"dbserver1\"," +
                "   \"mongodb.user\" : \"debezium\"," +
                "   \"mongodb.password\" : \"dbz\"," +
                "   \"database.history.kafka.bootstrap.servers\" : \"localhost:9092\"}" +
                "}";

        KafkaConnectS2IUtils.waitForConnectS2IStatus(kafkaConnectS2IName, "Ready");

        // Make sure that Connenct API is ready
        // TODO remove this sleep when ENTMQST-1613 will be fixed
        Thread.sleep(10_000);

        String createConnectorOutput = cmdKubeClient().execInPod(podForExecName, "curl", "-X", "POST", "-H", "Accept:application/json", "-H", "Content-Type:application/json",
                "http://" + KafkaConnectS2IResources.serviceName(kafkaConnectS2IName) + ":8083/connectors/", "-d", mongoDbConfig).out();
        LOGGER.info("Create Connector result: {}", createConnectorOutput);

        // Make sure that connector is really created
        Thread.sleep(10_000);

        String connectorStatus = cmdKubeClient().execInPod(podForExecName, "curl", "-X", "GET", "http://" + KafkaConnectS2IResources.serviceName(kafkaConnectS2IName) + ":8083/connectors/" + kafkaConnectS2IName + "/status").out();
        assertThat(connectorStatus, containsString("RUNNING"));
    }

    @Test
    void testDeployS2IAndKafkaConnectorWithMongoDBPlugin() throws InterruptedException {
        final String kafkaConnectS2IName = "kafka-connect-s2i-name-11";
        // Calls to Connect API are executed from kafka-0 pod
        String podForExecName = deployConnectS2IWithMongoDb(kafkaConnectS2IName);

        // Make sure that Connenct API is ready
        // TODO remove this sleep when ENTMQST-1613 will be fixed
        Thread.sleep(10_000);

        KafkaConnectorResource.kafkaConnector(kafkaConnectS2IName)
            .withNewSpec()
                .withClassName("io.debezium.connector.mongodb.MongoDbConnector")
                .withTasksMax(2)
                .addToConfig("mongodb.hosts", "debezium/localhost:27017")
                .addToConfig("mongodb.name", "dbserver1")
                .addToConfig("mongodb.user", "debezium")
                .addToConfig("mongodb.password", "dbz")
                .addToConfig("database.history.kafka.bootstrap.servers", "localhost:9092")
            .endSpec().done();

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);
        KafkaConnectUtils.waitForConnectorReady(kafkaConnectS2IName);

        String connectorStatus = cmdKubeClient().execInPod(podForExecName, "curl", "-X", "GET", "http://" + KafkaConnectS2IResources.serviceName(kafkaConnectS2IName) + ":8083/connectors/" + kafkaConnectS2IName + "/status").out();
        assertThat(connectorStatus, containsString("RUNNING"));
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testSecretsWithKafkaConnectS2IWithTlsAndScramShaAuthentication() throws Exception {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewTls()
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                        .endTls()
                        .withNewKafkaListenerExternalNodePort()
                            .withNewKafkaListenerAuthenticationScramSha512Auth()
                            .endKafkaListenerAuthenticationScramSha512Auth()
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();


        final String userName = "user-example-one";
        final String kafkaConnectS2IName = "kafka-connect-s2i-name-2";

        KafkaUserResource.scramShaUser(CLUSTER_NAME, userName).done();

        SecretUtils.waitForSecretReady(userName);

        KafkaConnectS2IResource.kafkaConnectS2I(kafkaConnectS2IName, CLUSTER_NAME, 1)
                .editMetadata()
                    .addToLabels("type", "kafka-connect-s2i")
                .endMetadata()
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .withNewTls()
                        .addNewTrustedCertificate()
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME))
                            .withCertificate("ca.crt")
                        .endTrustedCertificate()
                    .endTls()
                    .withBootstrapServers(KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
                    .withNewKafkaClientAuthenticationScramSha512()
                        .withUsername(userName)
                        .withNewPasswordSecret()
                            .withSecretName(userName)
                            .withPassword("password")
                        .endPasswordSecret()
                    .endKafkaClientAuthenticationScramSha512()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, CONNECT_S2I_TOPIC_NAME).done();

        String kafkaConnectS2IPodName = kubeClient().listPods("type", "kafka-connect-s2i").get(0).getMetadata().getName();
        String kafkaConnectS2ILogs = kubeClient().logs(kafkaConnectS2IPodName);

        LOGGER.info("Verifying that in kafka connect logs are everything fine");
        assertThat(kafkaConnectS2ILogs, not(containsString("ERROR")));

        LOGGER.info("Creating FileStreamSink connector in pod {} with topic {}", kafkaConnectS2IPodName, CONNECT_S2I_TOPIC_NAME);
        KafkaConnectUtils.createFileSinkConnector(kafkaConnectS2IPodName, CONNECT_S2I_TOPIC_NAME);

        waitForClusterAvailabilityScramSha(userName, NAMESPACE, CLUSTER_NAME, CONNECT_S2I_TOPIC_NAME, 2);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectS2IPodName);

        assertThat(cmdKubeClient().execInPod(kafkaConnectS2IPodName, "/bin/bash", "-c", "cat /tmp/test-file-sink.txt").out(),
                containsString("0\n1\n"));
    }

    @Test
    void testCustomAndUpdatedValues() {
        final String kafkaConnectS2IName = "kafka-connect-s2i-name-3";
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        LinkedHashMap<String, String> envVarGeneral = new LinkedHashMap<>();
        envVarGeneral.put("TEST_ENV_1", "test.env.one");
        envVarGeneral.put("TEST_ENV_2", "test.env.two");

        KafkaConnectS2IResource.kafkaConnectS2I(kafkaConnectS2IName, CLUSTER_NAME, 1)
            .editMetadata()
                .addToLabels("type", "kafka-connect-s2i")
            .endMetadata()
            .editSpec()
                .withNewTemplate()
                    .withNewConnectContainer()
                        .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                    .endConnectContainer()
                .endTemplate()
            .endSpec()
            .done();

        LinkedHashMap<String, String> envVarUpdated = new LinkedHashMap<>();
        envVarUpdated.put("TEST_ENV_2", "updated.test.env.two");
        envVarUpdated.put("TEST_ENV_3", "test.env.three");

        Map<String, String> connectSnapshot = DeploymentUtils.depConfigSnapshot(KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName));

        LOGGER.info("Verify values before update");

        LabelSelector deploymentConfigSelector = new LabelSelectorBuilder().addToMatchLabels(kubeClient().getDeploymentConfigSelectors(KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName))).build();
        String connectPodName = kubeClient().listPods(deploymentConfigSelector).get(0).getMetadata().getName();

        checkSpecificVariablesInContainer(connectPodName, KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName), envVarGeneral);

        LOGGER.info("Updating values in ConnectS2I container");
        KafkaConnectS2IResource.replaceConnectS2IResource(kafkaConnectS2IName, kc -> {
            kc.getSpec().getTemplate().getConnectContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
        });

        DeploymentUtils.waitTillDepConfigHasRolled(kafkaConnectS2IName, connectSnapshot);

        deploymentConfigSelector = new LabelSelectorBuilder().addToMatchLabels(kubeClient().getDeploymentConfigSelectors(KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName))).build();
        connectPodName = kubeClient().listPods(deploymentConfigSelector).get(0).getMetadata().getName();

        LOGGER.info("Verify values after update");
        checkSpecificVariablesInContainer(connectPodName, KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName), envVarUpdated);
    }

    @Test
    void testJvmAndResources() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        final String kafkaConnectS2IName = "kafka-connect-s2i-name-4";

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        KafkaConnectS2I kafkaConnectS2i = KafkaConnectS2IResource.kafkaConnectS2IWithoutWait(KafkaConnectS2IResource.defaultKafkaConnectS2I(kafkaConnectS2IName, CLUSTER_NAME, 1)
            .editMetadata()
                .addToLabels("type", "kafka-connect")
            .endMetadata()
            .editSpec()
                .withResources(new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity("400M"))
                        .addToLimits("cpu", new Quantity("2"))
                        .addToRequests("memory", new Quantity("300M"))
                        .addToRequests("cpu", new Quantity("1"))
                        .build())
                .withBuildResources(new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity("1000M"))
                        .addToLimits("cpu", new Quantity("1000"))
                        .addToRequests("memory", new Quantity("400M"))
                        .addToRequests("cpu", new Quantity("1000"))
                        .build())
                .withNewJvmOptions()
                    .withXmx("200m")
                    .withXms("200m")
                    .withServer(true)
                    .withXx(jvmOptionsXX)
                .endJvmOptions()
            .endSpec().build());

        KafkaConnectS2IUtils.waitForConnectS2IStatus(kafkaConnectS2IName, "NotReady");

        TestUtils.waitFor("build status: Pending", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_AVAILABILITY_TEST,
            () -> kubeClient().getClient().adapt(OpenShiftClient.class).builds().inNamespace(NAMESPACE).withName(kafkaConnectS2IName + "-connect-1").get().getStatus().getPhase().equals("Pending"));

        kubeClient().getClient().adapt(OpenShiftClient.class).builds().inNamespace(NAMESPACE).withName(kafkaConnectS2IName + "-connect-1").cascading(true).delete();

        KafkaConnectS2IResource.replaceConnectS2IResource(kafkaConnectS2IName, kc -> {
            kc.getSpec().setBuildResources(new ResourceRequirementsBuilder()
                    .addToLimits("memory", new Quantity("1000M"))
                    .addToLimits("cpu", new Quantity("1"))
                    .addToRequests("memory", new Quantity("400M"))
                    .addToRequests("cpu", new Quantity("1"))
                    .build());
        });

        TestUtils.waitFor("Kafka Connect CR change", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getClient().adapt(OpenShiftClient.class).buildConfigs().inNamespace(NAMESPACE).withName(kafkaConnectS2IName + "-connect").get().getSpec().getResources().getRequests().get("cpu").equals(new Quantity("1")));

        cmdKubeClient().exec("oc", "start-build", KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName), "-n", NAMESPACE);

        KafkaConnectS2IUtils.waitForConnectS2IStatus(kafkaConnectS2IName, "Ready");

        String podName = PodUtils.getPodNameByPrefix(kafkaConnectS2IName);

        assertResources(NAMESPACE, podName, kafkaConnectS2IName + "-connect",
                "400M", "2", "300M", "1");
        assertExpectedJavaOpts(podName, kafkaConnectS2IName + "-connect",
                "-Xmx200m", "-Xms200m", "-server", "-XX:+UseG1GC");

        KafkaConnectS2IResource.deleteKafkaConnectS2IWithoutWait(kafkaConnectS2i);
    }

    private String deployConnectS2IWithMongoDb(String kafkaConnectS2IName) {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        KafkaConnectS2IResource.kafkaConnectS2I(kafkaConnectS2IName, CLUSTER_NAME, 1)
            .editMetadata()
                .addToLabels("type", "kafka-connect-s2i")
                .addToAnnotations("strimzi.io/use-connector-resources", "true")
            .endMetadata()
            .done();

        Map<String, String> connectSnapshot = DeploymentUtils.depConfigSnapshot(KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName));

        File dir = FileUtils.downloadAndUnzip("https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/0.7.5/debezium-connector-mongodb-0.7.5-plugin.zip");

        // Start a new image build using the plugins directory
        cmdKubeClient().execInCurrentNamespace("start-build", KafkaConnectS2IResources.deploymentName(kafkaConnectS2IName), "--from-dir", dir.getAbsolutePath());
        // Wait for rolling update connect pods
        DeploymentUtils.waitTillDepConfigHasRolled(kafkaConnectS2IName, connectSnapshot);
        String podForExecName = KafkaResources.kafkaPodName(CLUSTER_NAME, 0);
        LOGGER.info("Collect plugins information from connect s2i pod");

        String plugins = cmdKubeClient().execInPod(podForExecName, "curl", "-X", "GET", "http://" + KafkaConnectS2IResources.serviceName(kafkaConnectS2IName) + ":8083/connector-plugins").out();

        assertThat(plugins, containsString("io.debezium.connector.mongodb.MongoDbConnector"));
        assertThat(kubeClient().getClient().adapt(OpenShiftClient.class).builds().inNamespace(NAMESPACE).list().getItems().size(), is(2));

        return podForExecName;
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT,  Constants.RECONCILIATION_INTERVAL).done();
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        super.recreateTestEnv(coNamespace, bindingsNamespaces, Constants.CO_OPERATION_TIMEOUT_SHORT);
    }
}
