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
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static io.strimzi.systemtest.TestTags.DYNAMIC_CONFIGURATION;
import static io.strimzi.systemtest.TestTags.REGRESSION;
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
    private static final Random RNG = new Random();

    private TestStorage suiteTestStorage;
    private String scraperPodName;

    private Stream<Arguments> dynamicConfiguration() {
        List<Arguments> testCases = new ArrayList<>();

        // per-broker config
        Map<String, String> perBrokerDynConfig = new HashMap<>();
        perBrokerDynConfig.put("ssl.protocol", "TLSv1.1");
        perBrokerDynConfig.put("ssl.enabled.protocols", "TLSv1.1");
        perBrokerDynConfig.put("principal.builder.class", "io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder");

        perBrokerDynConfig.forEach((key, value) -> testCases.add(Arguments.of(Scope.PER_BROKER.toString(), key, value)));

        // cluster-wide config
        Map<String, String> clusterWideDynConfig = new HashMap<>();
        clusterWideDynConfig.put("log.cleanup.policy", "compact");
        clusterWideDynConfig.put("log.message.timestamp.type", "LogAppendTime");
        clusterWideDynConfig.put("log.preallocate", "true");

        clusterWideDynConfig.forEach((key, value) -> testCases.add(Arguments.of(Scope.CLUSTER_WIDE.toString(), key, value)));

        return testCases.stream();
    }

    @TestDoc(
        description = @Desc(
            "Parametrized test taking 3 pre-defined per-broker and 3 pre-defined cluster-wide configurations that are being tested to see if dynamic configuration works." +
            "For each of the configuration (and its value), it goes through following steps:" +
            "\n 1. Apply the configuration" +
            "\n 2. Wait for stability of the cluster - no Pods will be rolled." +
            "\n 3. Verify that configuration is correctly set in CR and either all Pods or for whole cluster (based on scope)."
        ),
        steps = {
            @Step(value = "Update configuration (with value) in Kafka.", expected = "Configuration is successfully updated."),
            @Step(value = "For one minute, periodically check that there is no rolling update of Kafka Pods.", expected = "No Kafka Pods will be rolled."),
            @Step(value = "Verify the applied configuration on both the Kafka CustomResource and the Kafka pods.", expected = "The applied configurations are correctly reflected in the Kafka CustomResource and the kafka pods.")
        },
        labels = {
            @Label(value = TestDocsLabels.DYNAMIC_CONFIGURATION),
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    @ParameterizedTest(name = "scope: {0}, configuration: {1} with value {2}")
    @MethodSource("dynamicConfiguration")
    void testDynamicConfiguration(String scope, String config, String value) {
        KafkaUtils.updateConfigurationWithStabilityWait(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getClusterName(), config, value);

        // verify phase
        assertThat(KafkaUtils.verifyCrDynamicConfiguration(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getClusterName(), config, value), is(true));
        assertThat(
            KafkaUtils.verifyPodDynamicConfiguration(
                Environment.TEST_SUITE_NAMESPACE,
                suiteTestStorage.getClusterName(),
                scraperPodName,
                scope,
                config,
                value
            ),
            is(true)
        );
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
            final int stochasticNumber = RNG.nextInt(testCaseKeys.size());
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
        suiteTestStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();

        LOGGER.info("Deploying shared Kafka across all test cases!");
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(), suiteTestStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(), suiteTestStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 3).build(),
            ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getScraperName()).build()
        );

        scraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, suiteTestStorage.getScraperName()).get(0).getMetadata().getName();
    }
}
