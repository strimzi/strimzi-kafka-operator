/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.InlineLoggingBuilder;
import io.strimzi.api.kafka.model.common.JvmOptionsBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.sova.SovaResources;
import io.strimzi.api.kafka.model.kafka.sova.SovaSpec;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.performance.gather.collectors.BaseMetricsCollector;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumer;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumerBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Map;

import static io.strimzi.systemtest.TestTags.METRICS;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueNotNull;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.createPatternAndCollect;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.createPatternAndCollectWithoutWait;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
@Tag(METRICS)
@SuiteDoc(
    description = @Desc("This test suite verifies the Sova consumer lag exporter configured through `spec.extraMetrics` in the Kafka custom resource. A single small Kafka cluster is shared by all test cases and each test case only changes the Sova configuration and restores it at the end."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator.", expected = "Cluster Operator is deployed."),
        @Step(value = "Deploy Kafka with Sova enabled and a scraper Pod.", expected = "Kafka and Sova are deployed."),
        @Step(value = "Create three topics and exchange messages on each of them with three consumer groups.", expected = "Topics and consumer groups exist and Sova exposes the first metrics.")
    },
    afterTestSteps = {
        @Step(value = "Common cleaning of all resources created by this test class.", expected = "All resources deleted.")
    },
    labels = {
        @Label(value = TestDocsLabels.METRICS),
    }
)
public class SovaST extends AbstractST {

    private static final String TOPIC_CHECKOUT = "sova-checkout";
    private static final String TOPIC_PAYMENTS = "sova-payments";
    private static final String TOPIC_AUDIT = "sova-audit";

    private static final String GROUP_ORDERS = "sova-orders";
    private static final String GROUP_BILLING = "sova-billing";
    private static final String GROUP_AUDIT = "sova-audit";

    private TestStorage testStorage;
    private BaseMetricsCollector sovaCollector;

    @IsolatedTest
    @TestDoc(
        description = @Desc("This test case checks that the group and topic regular expressions are applied to the whole name and that the exclude expressions win over the include ones."),
        steps = {
            @Step(value = "Set `groupRegex` to `sova-or.*` and `topicRegex` to `sova-checkout` and wait for the Sova rolling update.", expected = "Only group `sova-orders` and topic `sova-checkout` are exposed."),
            @Step(value = "Set `topicRegex` to `sova` (a prefix of the topic names, not a full match).", expected = "No topic is exposed, because the expression must match the whole name."),
            @Step(value = "Set `groupRegex` to `sova-.*` with `groupExcludeRegex` `sova-audit` and `topicRegex` to `sova-.*` with `topicExcludeRegex` `sova-audit`.", expected = "Group and topic `sova-audit` are not exposed, the others are."),
            @Step(value = "Restore the default configuration.", expected = "Sova rolls and exposes everything again.")
        },
        labels = {
            @Label(value = TestDocsLabels.METRICS),
        }
    )
    void testRegexFilters() {
        final String namespaceName = testStorage.getNamespaceName();
        final String clusterName = testStorage.getClusterName();
        final String sovaName = SovaResources.componentName(clusterName);
        Map<String, String> snapshot = DeploymentUtils.depSnapshot(namespaceName, sovaName);

        KafkaUtils.replace(namespaceName, clusterName, kafka -> {
            kafka.getSpec().getExtraMetrics().setGroupRegex("sova-or.*");
            kafka.getSpec().getExtraMetrics().setTopicRegex(TOPIC_CHECKOUT);
        });
        snapshot = DeploymentUtils.waitTillDepHasRolled(namespaceName, sovaName, 1, snapshot);
        waitForMetric("kafka_consumergroup_current_offset\\{.*consumergroup=\"" + GROUP_ORDERS + "\".*\\}", true);
        waitForMetric("kafka_consumergroup_current_offset\\{.*consumergroup=\"" + GROUP_BILLING + "\".*\\}", false);
        waitForMetric("kafka_topic_partitions\\{topic=\"" + TOPIC_CHECKOUT + "\"\\}", true);
        waitForMetric("kafka_topic_partitions\\{topic=\"" + TOPIC_PAYMENTS + "\"\\}", false);

        // "sova" is only a prefix of the topic names, so with a full match nothing should be exposed
        KafkaUtils.replace(namespaceName, clusterName, kafka -> kafka.getSpec().getExtraMetrics().setTopicRegex("sova"));
        snapshot = DeploymentUtils.waitTillDepHasRolled(namespaceName, sovaName, 1, snapshot);
        waitForMetric("kafka_topic_partitions\\{topic=\"" + TOPIC_CHECKOUT + "\"\\}", false);

        KafkaUtils.replace(namespaceName, clusterName, kafka -> {
            kafka.getSpec().getExtraMetrics().setGroupRegex("sova-.*");
            kafka.getSpec().getExtraMetrics().setGroupExcludeRegex(GROUP_AUDIT);
            kafka.getSpec().getExtraMetrics().setTopicRegex("sova-.*");
            kafka.getSpec().getExtraMetrics().setTopicExcludeRegex(TOPIC_AUDIT);
        });
        snapshot = DeploymentUtils.waitTillDepHasRolled(namespaceName, sovaName, 1, snapshot);
        waitForMetric("kafka_consumergroup_current_offset\\{.*consumergroup=\"" + GROUP_BILLING + "\".*\\}", true);
        waitForMetric("kafka_consumergroup_current_offset\\{.*consumergroup=\"" + GROUP_AUDIT + "\".*\\}", false);
        waitForMetric("kafka_topic_partitions\\{topic=\"" + TOPIC_PAYMENTS + "\"\\}", true);
        waitForMetric("kafka_topic_partitions\\{topic=\"" + TOPIC_AUDIT + "\"\\}", false);

        KafkaUtils.replace(namespaceName, clusterName, kafka -> {
            kafka.getSpec().getExtraMetrics().setGroupRegex(".*");
            kafka.getSpec().getExtraMetrics().setGroupExcludeRegex(null);
            kafka.getSpec().getExtraMetrics().setTopicRegex(".*");
            kafka.getSpec().getExtraMetrics().setTopicExcludeRegex(null);
        });
        DeploymentUtils.waitTillDepHasRolled(namespaceName, sovaName, 1, snapshot);
        waitForMetric("kafka_topic_partitions\\{topic=\"" + TOPIC_AUDIT + "\"\\}", true);
    }

    @IsolatedTest
    @TestDoc(
        description = @Desc("This test case checks that Sova can be disabled and enabled again by removing and adding `spec.extraMetrics`."),
        steps = {
            @Step(value = "Remove `spec.extraMetrics` from the Kafka custom resource.", expected = "Sova Deployment is deleted."),
            @Step(value = "Add `spec.extraMetrics` back.", expected = "Sova Deployment is created and exposes the metrics again.")
        },
        labels = {
            @Label(value = TestDocsLabels.METRICS),
        }
    )
    void testEnableDisable() {
        final String sovaName = SovaResources.componentName(testStorage.getClusterName());

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> kafka.getSpec().setExtraMetrics(null));
        DeploymentUtils.waitForDeploymentDeletion(testStorage.getNamespaceName(), sovaName);

        KafkaUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), kafka -> kafka.getSpec().setExtraMetrics(new SovaSpec()));
        DeploymentUtils.waitForDeploymentAndPodsReady(testStorage.getNamespaceName(), sovaName, 1);

        // The Pod was recreated, so the metrics have to be collected again from the new Pod
        sovaCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
        assertMetricValueNotNull(sovaCollector, "kafka_topic_partitions\\{topic=\"" + TOPIC_CHECKOUT + "\"\\}");
    }

    @IsolatedTest
    @TestDoc(
        description = @Desc("This test case checks that the memory limit and `jvmOptions` are passed to the Sova JVM."),
        steps = {
            @Step(value = "Set a memory limit of 256Mi and wait for the Sova rolling update.", expected = "Sova starts with the heap derived from the memory limit (-Xmx set)."),
            @Step(value = "Set `jvmOptions` with `-Xmx` of 128m and `-XX` option `MaxMetaspaceSize` of 128m.", expected = "Sova starts with the configured options."),
            @Step(value = "Restore the default configuration.", expected = "Sova rolls back to the default configuration.")
        },
        labels = {
            @Label(value = TestDocsLabels.METRICS),
        }
    )
    void testJvmAndMemoryOptions() {
        final String namespaceName = testStorage.getNamespaceName();
        final String clusterName = testStorage.getClusterName();
        final String sovaName = SovaResources.componentName(clusterName);
        Map<String, String> snapshot = DeploymentUtils.depSnapshot(namespaceName, sovaName);

        KafkaUtils.replace(namespaceName, clusterName, kafka -> kafka.getSpec().getExtraMetrics().setResources(
            new ResourceRequirementsBuilder()
                .addToLimits("memory", new Quantity("256Mi"))
                .addToRequests("memory", new Quantity("256Mi"))
                .build()));
        snapshot = DeploymentUtils.waitTillDepHasRolled(namespaceName, sovaName, 1, snapshot);

        String podName = PodUtils.getPodNameByPrefix(namespaceName, sovaName);
        assertThat("Heap is not derived from the memory limit",
            StUtils.getLogFromPodByTime(namespaceName, podName, sovaName, "10m"), containsString("-Xmx"));

        KafkaUtils.replace(namespaceName, clusterName, kafka -> {
            kafka.getSpec().getExtraMetrics().setResources(null);
            kafka.getSpec().getExtraMetrics().setJvmOptions(new JvmOptionsBuilder()
                .withXmx("128m")
                .withXx(Map.of("MaxMetaspaceSize", "128m"))
                .build());
        });
        snapshot = DeploymentUtils.waitTillDepHasRolled(namespaceName, sovaName, 1, snapshot);

        podName = PodUtils.getPodNameByPrefix(namespaceName, sovaName);
        String startLog = StUtils.getLogFromPodByTime(namespaceName, podName, sovaName, "10m");
        assertThat("Xmx from jvmOptions is missing", startLog, containsString("-Xmx128m"));
        assertThat("XX option from jvmOptions is missing", startLog, containsString("-XX:MaxMetaspaceSize=128m"));

        KafkaUtils.replace(namespaceName, clusterName, kafka -> kafka.getSpec().getExtraMetrics().setJvmOptions(null));
        DeploymentUtils.waitTillDepHasRolled(namespaceName, sovaName, 1, snapshot);
    }

    @IsolatedTest
    @TestDoc(
        description = @Desc("This test case checks that a change of the Sova logging is applied without rolling the Sova Pod."),
        steps = {
            @Step(value = "Change the root logger level to DEBUG using inline logging.", expected = "The Sova Pod is not rolled."),
            @Step(value = "Wait for the logging configuration to be reloaded.", expected = "Sova prints DEBUG messages."),
            @Step(value = "Remove the logging configuration.", expected = "The Sova Pod is not rolled.")
        },
        labels = {
            @Label(value = TestDocsLabels.METRICS),
        }
    )
    void testLoggingChange() {
        final String namespaceName = testStorage.getNamespaceName();
        final String clusterName = testStorage.getClusterName();
        final String sovaName = SovaResources.componentName(clusterName);
        final Map<String, String> snapshot = DeploymentUtils.depSnapshot(namespaceName, sovaName);
        final String podName = PodUtils.getPodNameByPrefix(namespaceName, sovaName);

        assertThat("DEBUG is logged before the change",
            StUtils.getLogFromPodByTime(namespaceName, podName, sovaName, "30s").contains(" DEBUG "),
            is(false));

        KafkaUtils.replace(namespaceName, clusterName, kafka -> kafka.getSpec().getExtraMetrics().setLogging(
            new InlineLoggingBuilder()
                .withLoggers(Map.of("rootLogger.level", "DEBUG"))
                .build()));

        // Log4j2 checks the configuration every 30 seconds, the Pod is not restarted
        TestUtils.waitFor("DEBUG logs from Sova", TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT,
            () -> StUtils.getLogFromPodByTime(namespaceName, podName, sovaName, "30s").contains(" DEBUG "));

        KafkaUtils.replace(namespaceName, clusterName, kafka -> kafka.getSpec().getExtraMetrics().setLogging(null));
        assertThat("Sova Pod should not roll, but did.", DeploymentUtils.depSnapshot(namespaceName, sovaName), is(snapshot));
    }

    /**
     * Waits until the metric is present (or absent). Absence is checked on freshly collected metrics, because Sova
     * removes metrics of filtered out topics and groups on the next collection cycle.
     */
    private void waitForMetric(String metric, boolean shouldBePresent) {
        if (shouldBePresent) {
            createPatternAndCollect(sovaCollector, metric);
        } else {
            TestUtils.waitFor("metric " + metric + " to disappear", TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT, () -> {
                sovaCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
                return createPatternAndCollectWithoutWait(sovaCollector, metric).isEmpty();
            });
            assertThat("Metric " + metric + " should not be exposed", createPatternAndCollectWithoutWait(sovaCollector, metric), is(empty()));
        }
    }

    @BeforeAll
    void setup() {
        assumeFalse(Environment.isNamespaceRbacScope());

        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();

        testStorage = new TestStorage(KubeResourceManager.get().getTestContext(), Environment.TEST_SUITE_NAMESPACE);
        final String scraperName = testStorage.getClusterName() + "-" + TestConstants.SCRAPER_NAME;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
                .editSpec()
                    .withNewExtraMetrics()
                    .endExtraMetrics()
                .endSpec()
                .build(),
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), scraperName).build()
        );

        for (String topic : new String[]{TOPIC_CHECKOUT, TOPIC_PAYMENTS, TOPIC_AUDIT}) {
            KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), topic, testStorage.getClusterName()).build());
        }

        exchangeMessages(TOPIC_CHECKOUT, GROUP_ORDERS, "orders");
        exchangeMessages(TOPIC_PAYMENTS, GROUP_BILLING, "billing");
        exchangeMessages(TOPIC_AUDIT, GROUP_AUDIT, "audit");

        sovaCollector = new BaseMetricsCollector.Builder()
            .withScraperPodName(KubeResourceManager.get().kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), scraperName).get(0).getMetadata().getName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withComponent(SovaMetricsComponent.create(testStorage.getNamespaceName(), testStorage.getClusterName()))
            .build();
        sovaCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
    }

    private void exchangeMessages(String topicName, String consumerGroup, String clientSuffix) {
        final KafkaProducerConsumer clients = new KafkaProducerConsumerBuilder()
            .withProducerName(testStorage.getProducerName() + "-" + clientSuffix)
            .withConsumerName(testStorage.getConsumerName() + "-" + clientSuffix)
            .withConsumerGroup(consumerGroup)
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(topicName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(100)
            .build();

        KubeResourceManager.get().createResourceWithWait(clients.getProducer().getJob(), clients.getConsumer().getJob());
        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName() + "-" + clientSuffix,
            testStorage.getProducerName() + "-" + clientSuffix, 100, false);
    }
}
