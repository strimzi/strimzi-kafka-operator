/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.dynamicconfiguration;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.Type;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.StrimziPodSetResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static io.strimzi.systemtest.TestTags.DYNAMIC_CONFIGURATION;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(DYNAMIC_CONFIGURATION)
@SuiteDoc(
    description = @Desc("DynamicConfigurationSharedST is responsible for verifying that changing dynamic Kafka configuration will not trigger a rolling update. Shared -> for each test case we use the same Kafka resource configuration."),
    beforeTestSteps = {
        @Step(value = "Run Cluster Operator installation.", expected = "Cluster Operator is installed."),
        @Step(value = "Deploy shared Kafka across all test cases.", expected = "Shared Kafka is deployed."),
        @Step(value = "Deploy scraper pod.", expected = "Scraper pod is deployed.")
    },
    labels = {
        @Label(value = TestDocsLabels.DYNAMIC_CONFIGURATION),
        @Label(value = TestDocsLabels.KAFKA)
    }
)
public class DynamicConfSharedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DynamicConfSharedST.class);

    private TestStorage suiteTestStorage;

    private String scraperPodName;
    private static Random rng = new Random();

    @TestDoc(
        description = @Desc("This test dynamically selects and applies three Kafka dynamic configuration properties to verify that the changes do not trigger a rolling update in the Kafka cluster. It applies the configurations, waits for stability, and then verifies that the new configuration is applied both to the CustomResource (CR) and the running Kafka pods."),
        steps = {
            @Step(value = "Randomly choose three configuration properties for dynamic update.", expected = "Three configurations are selected without duplication."),
            @Step(value = "Apply the chosen configuration properties to the Kafka CustomResource.", expected = "The configurations are applied successfully without triggering a rolling update."),
            @Step(value = "Verify the applied configuration on both the Kafka CustomResource and the Kafka pods.", expected = "The applied configurations are correctly reflected in the Kafka CustomResource and the kafka pods.")
        },
        labels = {
            @Label(value = TestDocsLabels.DYNAMIC_CONFIGURATION),
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    @TestFactory
    Iterator<DynamicTest> testDynConfiguration() {

        List<DynamicTest> dynamicTests = new ArrayList<>(40);

        Map<String, Object> testCases = generateTestCases(TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).version());
        List<String> chosenTestCases = stochasticSelection(testCases);

        for (String key : chosenTestCases) {
            final Object value = testCases.get(key);

            dynamicTests.add(DynamicTest.dynamicTest("Test " + key + "->" + value, () -> {
                // exercise phase
                KafkaUtils.updateConfigurationWithStabilityWait(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getClusterName(), key, value);

                // verify phase
                assertThat(KafkaUtils.verifyCrDynamicConfiguration(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getClusterName(), key, value), is(true));
                assertThat(KafkaUtils.verifyPodDynamicConfiguration(Environment.TEST_SUITE_NAMESPACE, scraperPodName,
                    KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()), StrimziPodSetResource.getBrokerComponentName(suiteTestStorage.getClusterName()), key, value), is(true));
            }));
        }

        return dynamicTests.iterator();
    }

    /**
     * Method, which dynamically generate test cases based on Kafka version
     * @param kafkaVersion specific kafka version
     * @return String generated test cases
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity"})
    private static Map<String, Object> generateTestCases(String kafkaVersion) {

        Map<String, ConfigModel> dynamicProperties = KafkaUtils.getDynamicConfigurationProperties(kafkaVersion);
        Map<String, Object> testCases = new HashMap<>();

        dynamicProperties.forEach((key, value) -> {

            Type type = value.getType();
            Object stochasticChosenValue;

            switch (type) {
                case STRING:
                    switch (key) {
                        case "compression.type":
                            List<String> compressionTypes = Arrays.asList("snappy", "gzip", "lz4", "zstd");

                            stochasticChosenValue = compressionTypes.get(ThreadLocalRandom.current().nextInt(0, compressionTypes.size() - 1));
                            break;
                        case "log.message.timestamp.type":
                            stochasticChosenValue = "LogAppendTime";
                            break;
                        case "ssl.protocol":
                            stochasticChosenValue = "TLSv1.1";
                            break;
                        default:
                            stochasticChosenValue = " ";
                    }
                    testCases.put(key, stochasticChosenValue);
                    break;
                case INT:
                case LONG:
                    switch (key) {
                        case "num.recovery.threads.per.data.dir":
                        case "log.cleaner.threads":
                        case "num.network.threads":
                        case "min.insync.replicas":
                        case "num.replica.fetchers":
                        case "num.partitions":
                            stochasticChosenValue = ThreadLocalRandom.current().nextInt(2, 3);
                            break;
                        case "log.cleaner.io.buffer.load.factor":
                        case "log.retention.ms":
                        case "max.connections":
                        case "max.connections.per.ip":
                        case "background.threads":
                            stochasticChosenValue = ThreadLocalRandom.current().nextInt(4, 20);
                            break;
                        default:
                            stochasticChosenValue = ThreadLocalRandom.current().nextInt(100, 50_000);
                    }
                    testCases.put(key, stochasticChosenValue);
                    break;
                case DOUBLE:
                    switch (key) {
                        case "log.cleaner.min.cleanable.dirty.ratio":
                        case "log.cleaner.min.cleanable.ratio":
                            stochasticChosenValue = ThreadLocalRandom.current().nextDouble(0, 1);
                            break;
                        default:
                            stochasticChosenValue = ThreadLocalRandom.current().nextDouble(1, 20);
                    }
                    testCases.put(key, stochasticChosenValue);
                    break;
                case BOOLEAN:
                    switch (key) {
                        case "unclean.leader.election.enable":
                        case "log.preallocate":
                            stochasticChosenValue = true;
                            break;
                        case "log.message.downconversion.enable":
                            stochasticChosenValue = false;
                            break;
                        default:
                            stochasticChosenValue = ThreadLocalRandom.current().nextInt(2) == 0;
                    }
                    testCases.put(key, stochasticChosenValue);
                    break;
                case LIST:
                    // metric.reporters has default empty '""'
                    // log.cleanup.policy = [delete, compact] -> default delete
                    switch (key) {
                        case "log.cleanup.policy":
                            stochasticChosenValue = "compact";
                            break;
                        case "ssl.enabled.protocols":
                            stochasticChosenValue = "TLSv1.1";
                            break;
                        default:
                            stochasticChosenValue = " ";
                    }
                    testCases.put(key, stochasticChosenValue);
            }

            // skipping these configuration, which doesn't work appear in the kafka pod (TODO: investigate why!)
            testCases.remove("num.recovery.threads.per.data.dir");
            testCases.remove("num.io.threads");
            testCases.remove("log.cleaner.dedupe.buffer.size");
            testCases.remove("num.partitions");

            // skipping these configuration exceptions
            testCases.remove("ssl.cipher.suites");
        });

        return testCases;
    }

    /**
     * Method, which randomly choose 3 dynamic properties for verification from {@param testCases}. In this case we are ok
     * with stochastic selection, because we don't care, which configuration is used. Furthermore, it's the same path
     * of code (i.e., same CFG (control flow graph), which does not triggers RollingUpdate).
     * @param testCases test cases, where each consist of one dynamic property
     * @return List 3 chosen dynamic properties
     */
    private static List<String> stochasticSelection(final Map<String, Object> testCases) {
        final List<String> testCaseKeys = new ArrayList<>(testCases.keySet());
        final List<String> chosenDynConfigurations = new ArrayList<>(3);

        for (int i = 0; i < 3; i++) {
            final int stochasticNumber = rng.nextInt(testCaseKeys.size());
            final String chosenDynConfiguration = testCaseKeys.get(stochasticNumber);

            LOGGER.debug("New configuration in List of dynamic configuration:\n{}", chosenDynConfigurations.toString());
            chosenDynConfigurations.add(chosenDynConfiguration);
            // remove it from `testCaseKeys` list to not include it twice
            testCaseKeys.remove(chosenDynConfiguration);
        }

        LOGGER.debug("Chosen dynamic configuration are:\n{}", chosenDynConfigurations.toString());

        return chosenDynConfigurations;
    }

    @BeforeAll
    void setup() {
        suiteTestStorage = new TestStorage(ResourceManager.getTestContext());

        SetupClusterOperator
            .get()
            .withDefaultConfiguration()
            .install();

        LOGGER.info("Deploying shared Kafka across all test cases!");
        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(), suiteTestStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(), suiteTestStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(
            KafkaTemplates.kafka(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 3).build(),
            ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getScraperName()).build()
        );

        scraperPodName = kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getScraperName()).get(0).getMetadata().getName();
    }
}
