/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.balancing.BrokerCapacity;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.WaitException;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
public class CruiseControlST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlST.class);
    private static final String NAMESPACE = "cruise-control-test";

    private static final String CRUISE_CONTROL_NAME = "Cruise Control";
    private static final int CRUISE_CONTROL_DEFAULT_PORT = 9090;
    private static final String CRUISE_CONTROL_STATE_ENDPOINT = "/kafkacruisecontrol/state";
    private static final String CRUISE_CONTROL_POD_PREFIX = CLUSTER_NAME + "-cruise-control-";

    private static final String CRUISE_CONTROL_CAPACITY_FILE_PATH = "/tmp/capacity.json";
    private static final String CRUISE_CONTROL_CONFIGURATION_FILE_PATH = "/tmp/cruisecontrol.properties";

    @Test
    void testCruiseControlDeployment()  {
        String ccStatusCommand = "curl -X GET localhost:" + CRUISE_CONTROL_DEFAULT_PORT + CRUISE_CONTROL_STATE_ENDPOINT;

        String ccPodName = PodUtils.getFirstPodNameContaining("cruise-control");

        String result = cmdKubeClient().execInPodContainer(ccPodName, "cruise-control",
            "/bin/bash", "-c", ccStatusCommand).out();

        LOGGER.info("Verifying that {} is running inside the Pod {} and REST API is available", CRUISE_CONTROL_NAME, ccPodName);

        assertThat(result, not(containsString("404")));
        assertThat(result, containsString("RUNNING"));

        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent();
    }

    @Test
    void testUninstallingAndInstallationCruiseControl() throws IOException {

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            LOGGER.info("Removing Cruise Control to the classic Kafka.");
            kafka.getSpec().setCruiseControl(null);
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        LOGGER.info("Verifying that in {} is not present in the Kafka cluster", CRUISE_CONTROL_NAME);
        assertThat(KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getCruiseControl(), nullValue());

        LOGGER.info("Verifying that {} pod is not present", CRUISE_CONTROL_NAME);
        assertThat(kubeClient().listPodsByPrefixInName(CRUISE_CONTROL_POD_PREFIX).size(), is(0));

        LOGGER.info("Verifying that in Kafka config map there is no configuration to cruise control metric reporter");
        assertThrows(WaitException.class, () -> CruiseControlUtils.verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(CruiseControlUtils.getKafkaCruiseControlMetricsReporterConfiguration(CLUSTER_NAME));

        LOGGER.info("Cruise Control topics will not be deleted and will stay in the Kafka cluster");
        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent();

        kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            LOGGER.info("Adding Cruise Control to the classic Kafka.");
            kafka.getSpec().setCruiseControl(new CruiseControlSpec());
        });

        LOGGER.info("Verifying that in Kafka config map there is configuration to cruise control metric reporter");
        CruiseControlUtils.verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(CruiseControlUtils.getKafkaCruiseControlMetricsReporterConfiguration(CLUSTER_NAME));

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        LOGGER.info("Verifying that {} topics are created after CC is instantiated.", CRUISE_CONTROL_NAME);

        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent();
    }

    @Test
    void testConfigurationChangeTriggersRollingUpdate() {

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            LOGGER.info("Adding Cruise Control to the classic Kafka.");
            BrokerCapacity brokerCapacity = kafka.getSpec().getCruiseControl().getBrokerCapacity();

            // TODO: this doesn't work because in the Kafka CR there is nothing using `kafka.getSpec().getCruiseControl()` only object,
            // TODO: which has some default values by they are not propagated to other fields...
            brokerCapacity.setDisk("20Gi");

            kafka.getSpec().getCruiseControl().setBrokerCapacity(brokerCapacity);
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent();
    }

    @Test
    void testConfigurationReflection() throws IOException {
        Pod cruiseControlPod = kubeClient().listPodsByPrefixInName(CRUISE_CONTROL_POD_PREFIX).get(0);

        String cruiseControlPodName = cruiseControlPod.getMetadata().getName();

        String configurationFileContent = cmdKubeClient().execInPod(cruiseControlPodName, "/bin/bash", "-c", "cat " + CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();

        InputStream configurationFileStream = new ByteArrayInputStream(configurationFileContent.getBytes(StandardCharsets.UTF_8));

        Properties fileConfiguration = new Properties();
        fileConfiguration.load(configurationFileStream);

        Container cruiseControlContainer = null;

        for (Container container : cruiseControlPod.getSpec().getContainers()) {
            if (container.getName().equals("cruise-control")) {
                cruiseControlContainer = container;
            }
        }

        EnvVar cruiseControlConfiguration = null;

        for (EnvVar envVar : Objects.requireNonNull(cruiseControlContainer).getEnv()) {
            if (envVar.getName().equals("CRUISE_CONTROL_CONFIGURATION")) {
                cruiseControlConfiguration = envVar;
            }
        }

        InputStream configurationContainerStream = new ByteArrayInputStream(Objects.requireNonNull(cruiseControlConfiguration).getValue().getBytes(StandardCharsets.UTF_8));

        Properties containerConfiguration = new Properties();
        containerConfiguration.load(configurationContainerStream);

        LOGGER.info("Verifying that all configuration in the cruise control container matching the cruise control file {} properties", CRUISE_CONTROL_CONFIGURATION_FILE_PATH);

        assertThat(containerConfiguration.getProperty("num.partition.metrics.windows"), is(fileConfiguration.getProperty("num.partition.metrics.windows")));
        assertThat(containerConfiguration.getProperty("completed.user.task.retention.time.ms"), is(fileConfiguration.getProperty("completed.user.task.retention.time.ms")));
        assertThat(containerConfiguration.getProperty("num.broker.metrics.windows"), is(fileConfiguration.getProperty("num.broker.metrics.windows")));
        assertThat(containerConfiguration.getProperty("broker.metrics.window.ms"), is(fileConfiguration.getProperty("broker.metrics.window.ms")));
        assertThat(containerConfiguration.getProperty("default.goals"), is(fileConfiguration.getProperty("default.goals")));
        assertThat(containerConfiguration.getProperty("partition.metrics.window.ms"), is(fileConfiguration.getProperty("partition.metrics.window.ms")));
        assertThat(containerConfiguration.getProperty("goals"), is(fileConfiguration.getProperty("goals")));

        // TODO: is there some way to fetch the configuration in the Kafka CR?? (it is default) -> but where i can find it
    }

    @Test
    void testCapacityFile() {

        String cruiseControlPodName = kubeClient().listPodsByPrefixInName(CRUISE_CONTROL_POD_PREFIX).get(0).getMetadata().getName();

        JsonObject cruiseControlCapacityFileContent =
            new JsonObject(cmdKubeClient().execInPod(cruiseControlPodName, "/bin/bash", "-c", "cat " + CRUISE_CONTROL_CAPACITY_FILE_PATH).out());

        assertThat(cruiseControlCapacityFileContent.getJsonObject("brokerCapacities"), not(nullValue()));

        LOGGER.info("We got only one configuration of broker-capacities");
        assertThat(cruiseControlCapacityFileContent.getJsonArray("brokerCapacities").size(), is(1));

        LOGGER.info("Verifying cruise control configuration.");

        JsonObject cruiseControlFirstConfiguration = cruiseControlCapacityFileContent.getJsonArray("brokerCapacities").getJsonObject(0);

        assertThat(cruiseControlFirstConfiguration.getString("brokerId"), is("-1"));
        assertThat(cruiseControlFirstConfiguration.getString("doc"), not(nullValue()));

        JsonObject cruiseControlConfigurationOfBrokerCapacity = cruiseControlFirstConfiguration.getJsonObject("capacity");

        LOGGER.info("Verifying default cruise control capacities");

        assertThat(cruiseControlConfigurationOfBrokerCapacity.getString("DISK"), is("100000.0"));
        assertThat(cruiseControlConfigurationOfBrokerCapacity.getString("CPU"), is("100"));
        assertThat(cruiseControlConfigurationOfBrokerCapacity.getString("NW_IN"), is("10000.0"));
        assertThat(cruiseControlConfigurationOfBrokerCapacity.getString("NW_OUT"), is("10000.0"));
    }

    @Test
    void testConfigurationFileIsCreated() {
        String cruiseControlPodName = kubeClient().listPodsByPrefixInName(CRUISE_CONTROL_POD_PREFIX).get(0).getMetadata().getName();

        String cruiseControlConfigurationFileContent = cmdKubeClient().execInPod(cruiseControlPodName, "/bin/bash", "-c", "cat " + CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();

        assertThat(cruiseControlConfigurationFileContent, not(nullValue()));
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();

        KafkaResource.kafkaWithCruiseControl(CLUSTER_NAME, 3, 3).done();
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) throws InterruptedException {
        super.recreateTestEnv(coNamespace, bindingsNamespaces);

        KafkaResource.kafkaWithCruiseControl(CLUSTER_NAME, 3, 3).done();
    }
}
