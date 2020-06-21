/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.CruiseControlSpecBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.WaitException;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CruiseControlConfigurationST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlConfigurationST.class);
    private static final String NAMESPACE = "cruise-control-configuration-test";

    private static final String CRUISE_CONTROL_NAME = "Cruise Control";
    private static final String CRUISE_CONTROL_POD_PREFIX = CLUSTER_NAME + "-cruise-control-";

    private static final String CRUISE_CONTROL_CAPACITY_FILE_PATH = "/tmp/capacity.json";
    private static final String CRUISE_CONTROL_CONFIGURATION_FILE_PATH = "/tmp/cruisecontrol.properties";

    @Order(1)
    @Test
    void testCapacityFile() {

        String cruiseControlPodName = kubeClient().listPodsByPrefixInName(CRUISE_CONTROL_POD_PREFIX).get(0).getMetadata().getName();

        JsonObject cruiseControlCapacityFileContent =
            new JsonObject(cmdKubeClient().execInPod(cruiseControlPodName, "/bin/bash", "-c", "cat " + CRUISE_CONTROL_CAPACITY_FILE_PATH).out());

        assertThat(cruiseControlCapacityFileContent.getJsonArray("brokerCapacities"), not(nullValue()));

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

    @Order(2)
    @Test
    void testDeployAndUnDeployCruiseControl() throws IOException {

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            LOGGER.info("Removing Cruise Control to the classic Kafka.");
            kafka.getSpec().setCruiseControl(null);
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        LOGGER.info("Verifying that in {} is not present in the Kafka cluster", CRUISE_CONTROL_NAME);
        assertThat(KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getCruiseControl(), nullValue());

        LOGGER.info("Verifying that {} pod is not present", CRUISE_CONTROL_POD_PREFIX);
        PodUtils.waitUntilPodStabilityReplicasCount(CRUISE_CONTROL_POD_PREFIX, 0);

        LOGGER.info("Verifying that in Kafka config map there is no configuration to cruise control metric reporter");
        assertThrows(WaitException.class, () -> CruiseControlUtils.verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(CruiseControlUtils.getKafkaCruiseControlMetricsReporterConfiguration(CLUSTER_NAME)));

        LOGGER.info("Cruise Control topics will not be deleted and will stay in the Kafka cluster");
        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent();

        kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            LOGGER.info("Adding Cruise Control to the classic Kafka.");
            kafka.getSpec().setCruiseControl(new CruiseControlSpec());
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        LOGGER.info("Verifying that in Kafka config map there is configuration to cruise control metric reporter");
        CruiseControlUtils.verifyCruiseControlMetricReporterConfigurationInKafkaConfigMapIsPresent(CruiseControlUtils.getKafkaCruiseControlMetricsReporterConfiguration(CLUSTER_NAME));

        LOGGER.info("Verifying that {} topics are created after CC is instantiated.", CRUISE_CONTROL_NAME);

        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent();
    }

    @Order(3)
    @Test
    void testConfigurationDiskChangeDoNotTriggersRollingUpdateOfKafkaPods() {

        Map<String, String> kafkaSnapShot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> cruiseControlSnapShot = DeploymentUtils.depSnapshot(CruiseControlResources.deploymentName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {

            LOGGER.info("Changing the broker capacity of the cruise control");

            CruiseControlSpec cruiseControl = new CruiseControlSpecBuilder()
                .withNewBrokerCapacity()
                    .withNewDisk("200M")
                .endBrokerCapacity()
                .build();

            kafka.getSpec().setCruiseControl(cruiseControl);
        });

        LOGGER.info("Verifying that CC pod is rolling, because of change size of disk");
        DeploymentUtils.waitTillDepHasRolled(CruiseControlResources.deploymentName(CLUSTER_NAME), 1, cruiseControlSnapShot);

        LOGGER.info("Verifying that Kafka pods did not roll");
        StatefulSetUtils.waitForNoRollingUpdate(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), kafkaSnapShot);

        LOGGER.info("Verifying new configuration in the Kafka CR");

        assertThat(KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec()
            .getCruiseControl().getBrokerCapacity().getDisk(), is("200M"));

        CruiseControlUtils.verifyThatCruiseControlTopicsArePresent();
    }

    @Order(4)
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
    }

    @Order(5)
    @Test
    void testConfigurationFileIsCreated() {
        String cruiseControlPodName = kubeClient().listPodsByPrefixInName(CRUISE_CONTROL_POD_PREFIX).get(0).getMetadata().getName();

        String cruiseControlConfigurationFileContent = cmdKubeClient().execInPod(cruiseControlPodName, "/bin/bash", "-c", "cat " + CRUISE_CONTROL_CONFIGURATION_FILE_PATH).out();

        assertThat(cruiseControlConfigurationFileContent, not(nullValue()));
    }
    
    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
//        prepareEnvForOperator(NAMESPACE);
//
//        applyRoleBindings(NAMESPACE);
//        // 050-Deployment
//        KubernetesResource.clusterOperator(NAMESPACE).done();
        installClusterOperator(NAMESPACE);

        deployTestResources();
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) throws Exception {
        super.recreateTestEnv(coNamespace, bindingsNamespaces);
        deployTestResources();
    }

    private void deployTestResources() {
        KafkaResource.kafkaWithCruiseControl(CLUSTER_NAME, 3, 3).done();
    }
}
