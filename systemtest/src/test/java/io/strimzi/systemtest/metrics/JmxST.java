/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.jmx.KafkaJmxAuthenticationPassword;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.FIPSNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.systemtest.utils.specific.JmxUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("This suite verifies JMX metrics behavior under various configurations and scenarios."),
    beforeTestSteps = {
        @Step(value = "Install the Cluster Operator and ensure environment readiness.", expected = "Cluster Operator is deployed and environment is ready.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA),
        @Label(value = TestDocsLabels.METRICS)
    }
)
public class JmxST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(JmxST.class);

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @FIPSNotSupported("JMX with auth is not working with FIPS")
    @TestDoc(
        description = @Desc("Test verifying Kafka and KafkaConnect with JMX authentication enabled and correct version reporting."),
        steps = {
            @Step(value = "Deploy a Kafka cluster (ephemeral storage) with JMX authentication.", expected = "Kafka cluster is deployed with JMX enabled and authentication set."),
            @Step(value = "Deploy a KafkaConnect cluster with JMX authentication.", expected = "KafkaConnect is deployed with JMX enabled and authentication set."),
            @Step(value = "Create a Scraper Pod, install jmxterm, and collect JMX metrics from both Kafka and KafkaConnect.", expected = "JMX metrics are retrieved; Kafka version is reported correctly.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA),
            @Label(value = TestDocsLabels.METRICS)
        }
    )
    void testKafkaAndKafkaConnectWithJMX() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String connectJmxSecretName = testStorage.getClusterName() + "-kafka-connect-jmx";
        final String kafkaJmxSecretName = testStorage.getClusterName() + "-kafka-jmx";

        Map<String, String> jmxSecretLabels = Collections.singletonMap("my-label", "my-value");
        Map<String, String> jmxSecretAnnotations = Collections.singletonMap("my-annotation", "some-value");

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        Kafka kafka = KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editOrNewSpec()
                .editKafka()
                    .withNewJmxOptions()
                        .withAuthentication(new KafkaJmxAuthenticationPassword())
                    .endJmxOptions()
                    .editOrNewTemplate()
                        .withNewJmxSecret()
                            .withNewMetadata()
                                .withLabels(jmxSecretLabels)
                                .withAnnotations(jmxSecretAnnotations)
                            .endMetadata()
                        .endJmxSecret()
                    .endTemplate()
                .endKafka()
            .endSpec()
            .build();

        KubeResourceManager.get().createResourceWithWait(kafka, ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build());
        String scraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();
        JmxUtils.downloadJmxTermToPod(testStorage.getNamespaceName(), scraperPodName);

        KafkaConnect connect = KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
            .editOrNewSpec()
                .withNewJmxOptions()
                    .withAuthentication(new KafkaJmxAuthenticationPassword())
                .endJmxOptions()
            .endSpec()
            .build();

        KubeResourceManager.get().createResourceWithWait(connect);

        LOGGER.info("Deploying NetworkPolicies for KafkaConnect");
        NetworkPolicyUtils.deployNetworkPolicyForResource(connect, KafkaConnectResources.componentName(testStorage.getClusterName()));

        String kafkaResults = JmxUtils.collectJmxMetricsWithWait(testStorage.getNamespaceName(), KafkaResources.brokersServiceName(testStorage.getClusterName()), kafkaJmxSecretName, scraperPodName, "bean kafka.server:type=app-info\nget -i *");
        String kafkaConnectResults = JmxUtils.collectJmxMetricsWithWait(testStorage.getNamespaceName(), KafkaConnectResources.serviceName(testStorage.getClusterName()), connectJmxSecretName, scraperPodName, "bean kafka.connect:type=app-info\nget -i *");
        assertThat("Result from Kafka JMX doesn't contain right version of Kafka, result: " + kafkaResults, kafkaResults, containsString("version = " + Environment.ST_KAFKA_VERSION));
        assertThat("Result from KafkaConnect JMX doesn't contain right version of Kafka, result: " + kafkaConnectResults, kafkaConnectResults, containsString("version = " + Environment.ST_KAFKA_VERSION));
    }

    @BeforeAll
    void setup() {
        final String namespaceToWatch = Environment.isNamespaceRbacScope() ? CO_NAMESPACE : TestConstants.WATCH_ALL_NAMESPACES;

        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_SHORT)
                .build()
            )
            .install();
    }
}
