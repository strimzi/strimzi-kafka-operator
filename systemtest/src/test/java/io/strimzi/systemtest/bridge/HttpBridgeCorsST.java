/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.annotations.TestTag;
import io.skodjob.annotations.UseCase;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeHttpCors;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.specific.BridgeUtils;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.BRIDGE;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@Tag(BRIDGE)
@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite for HTTP Bridge CORS functionality, focusing on verifying correct handling of allowed and forbidden origins."),
    beforeTestSteps = {
        @Step(value = "Set up Kafka Bridge and its configuration including CORS settings", expected = "Kafka Bridge is set up with the correct configuration"),
        @Step(value = "Deploy required Kafka resources and scraper pod", expected = "Kafka resources and scraper pod are deployed and running")
    },
    useCases = {
        @UseCase(id = "handle-cors-requests"),
        @UseCase(id = "security-validation")
    },
    tags = {
        @TestTag(value = BRIDGE),
        @TestTag(value = REGRESSION)
    }
)
public class HttpBridgeCorsST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeCorsST.class);
    private static final String ALLOWED_ORIGIN = "https://strimzi.io";
    private static final String NOT_ALLOWED_ORIGIN = "https://evil.io";

    private String bridgeUrl;
    private TestStorage suiteTestStorage;

    @ParallelTest
    @TestDoc(
        description = @Desc("This test checks if CORS handling for allowed origin works correctly in the Kafka Bridge."),
        steps = {
            @Step(value = "Set up the Kafka Bridge user and configuration", expected = "Kafka Bridge user and configuration are set up"),
            @Step(value = "Construct the request URL and headers", expected = "URL and headers are constructed properly"),
            @Step(value = "Send OPTIONS request to Kafka Bridge and capture the response", expected = "Response is captured from Bridge"),
            @Step(value = "Validate the response contains expected status codes and headers", expected = "Response has correct status codes and headers for allowed origin"),
            @Step(value = "Send GET request to Kafka Bridge and capture the response", expected = "Response is captured from Bridge for GET request"),
            @Step(value = "Check if the GET request response is '404 Not Found'", expected = "Response for GET request is 404 Not Found")
        },
        useCases = {
            @UseCase(id = "validate_cors_handling_allowed_origin")
        },
        tags = {
            @TestTag(value = BRIDGE),
            @TestTag(value = REGRESSION)
        }
    )
    void testCorsOriginAllowed() {
        final String kafkaBridgeUser = "bridge-user-example";
        final String groupId = ClientUtils.generateRandomConsumerGroup();

        JsonObject config = new JsonObject();
        config.put("name", kafkaBridgeUser);
        config.put("format", "json");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<String, String> additionalHeaders = new HashMap<>();
        additionalHeaders.put("Origin", ALLOWED_ORIGIN);
        additionalHeaders.put("Access-Control-Request-Method", HttpMethod.POST.toString());

        String url = bridgeUrl + "/consumers/" + groupId + "/instances/" + kafkaBridgeUser + "/subscription";
        String headers = BridgeUtils.addHeadersToString(additionalHeaders, TestConstants.KAFKA_BRIDGE_JSON_JSON);
        String response = cmdKubeClient().namespace(suiteTestStorage.getNamespaceName()).execInPod(suiteTestStorage.getScraperPodName(), "/bin/bash", "-c", BridgeUtils.buildCurlCommand(HttpMethod.OPTIONS, url, headers, "")).out().trim();
        LOGGER.info("Response from Bridge: {}", response);

        String responseAllowHeaders = BridgeUtils.getHeaderValue("access-control-allow-headers", response);
        LOGGER.info("Checking if response from Bridge is correct");

        assertThat(response.contains("200 OK") || response.contains("204 No Content"), is(true));
        assertThat(BridgeUtils.getHeaderValue("access-control-allow-origin", response), is(ALLOWED_ORIGIN));
        assertThat(responseAllowHeaders, containsString("access-control-allow-origin"));
        assertThat(responseAllowHeaders, containsString("origin"));
        assertThat(responseAllowHeaders, containsString("x-requested-with"));
        assertThat(responseAllowHeaders, containsString("content-type"));
        assertThat(responseAllowHeaders, containsString("access-control-allow-methods"));
        assertThat(responseAllowHeaders, containsString("accept"));
        assertThat(BridgeUtils.getHeaderValue("access-control-allow-methods", response), containsString(HttpMethod.POST.toString()));

        url = bridgeUrl + "/consumers/" + groupId + "/instances/" + kafkaBridgeUser + "/subscription";
        headers = BridgeUtils.addHeadersToString(Collections.singletonMap("Origin", ALLOWED_ORIGIN));
        response = cmdKubeClient().namespace(suiteTestStorage.getNamespaceName()).execInPod(suiteTestStorage.getScraperPodName(), "/bin/bash", "-c", BridgeUtils.buildCurlCommand(HttpMethod.GET, url, headers, "")).out().trim();
        LOGGER.info("Response from Bridge: {}", response);

        assertThat(response, containsString("404"));
    }

    @ParallelTest
    @TestDoc(
        description = @Desc("Test ensuring that CORS (Cross-Origin Resource Sharing) requests with forbidden origins are correctly rejected by the Bridge."),
        steps = {
            @Step(value = "Create Kafka Bridge user and consumer group", expected = "Kafka Bridge user and consumer group are created successfully"),
            @Step(value = "Set up headers with forbidden origin and pre-flight HTTP OPTIONS method", expected = "Headers and method are set correctly"),
            @Step(value = "Send HTTP OPTIONS request to the Bridge", expected = "HTTP OPTIONS request is sent to the Bridge and a response is received"),
            @Step(value = "Verify the response contains '403' and 'CORS Rejected - Invalid origin'", expected = "Response indicates the CORS request is rejected"),
            @Step(value = "Remove 'Access-Control-Request-Method' from headers and set HTTP POST method", expected = "Headers are updated and HTTP method is set correctly"),
            @Step(value = "Send HTTP POST request to the Bridge", expected = "HTTP POST request is sent to the Bridge and a response is received"),
            @Step(value = "Verify the response contains '403' and 'CORS Rejected - Invalid origin'", expected = "Response indicates the CORS request is rejected")
        },
        useCases = {
            @UseCase(id = "handle-cors-requests"),
            @UseCase(id = "security-validation")
        },
        tags = {
            @TestTag(value = BRIDGE),
            @TestTag(value = REGRESSION)
        }
    )
    void testCorsForbidden() {
        final String kafkaBridgeUser = "bridge-user-example";
        final String groupId = ClientUtils.generateRandomConsumerGroup();

        Map<String, String> additionalHeaders = new HashMap<>();
        additionalHeaders.put("Origin", NOT_ALLOWED_ORIGIN);
        additionalHeaders.put("Access-Control-Request-Method", HttpMethod.POST.toString());

        String url = bridgeUrl + "/consumers/" + groupId + "/instances/" + kafkaBridgeUser + "/subscription";
        String headers = BridgeUtils.addHeadersToString(additionalHeaders);
        String response = cmdKubeClient().namespace(suiteTestStorage.getNamespaceName()).execInPod(suiteTestStorage.getScraperPodName(), "/bin/bash", "-c", BridgeUtils.buildCurlCommand(HttpMethod.OPTIONS, url, headers, "")).out().trim();
        LOGGER.info("Response from Bridge: {}", response);

        LOGGER.info("Checking if response from Bridge is correct");
        assertThat(response, containsString("403"));
        assertThat(response, containsString("CORS Rejected - Invalid origin"));

        additionalHeaders.remove("Access-Control-Request-Method", HttpMethod.POST.toString());
        headers = BridgeUtils.addHeadersToString(additionalHeaders);
        response = cmdKubeClient().namespace(suiteTestStorage.getNamespaceName()).execInPod(suiteTestStorage.getScraperPodName(), "/bin/bash", "-c", BridgeUtils.buildCurlCommand(HttpMethod.POST, url, headers, "")).out().trim();
        LOGGER.info("Response from Bridge: {}", response);

        LOGGER.info("Checking if response from Bridge is correct");
        assertThat(response, containsString("403"));
        assertThat(response, containsString("CORS Rejected - Invalid origin"));
    }

    @BeforeAll
    void beforeAll() {
        suiteTestStorage = new TestStorage(ResourceManager.getTestContext());

        clusterOperator = clusterOperator.defaultInstallation()
                .createInstallation()
                .runInstallation();

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getBrokerPoolName(), suiteTestStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(suiteTestStorage.getNamespaceName(), suiteTestStorage.getControllerPoolName(), suiteTestStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(suiteTestStorage.getNamespaceName(), suiteTestStorage.getClusterName(), 1, 3).build());

        resourceManager.createResourceWithWait(ScraperTemplates.scraperPod(suiteTestStorage.getNamespaceName(), suiteTestStorage.getScraperName()).build());
        suiteTestStorage.addToTestStorage(TestConstants.SCRAPER_POD_KEY, kubeClient().listPodsByPrefixInName(suiteTestStorage.getNamespaceName(), suiteTestStorage.getScraperName()).get(0).getMetadata().getName());

        resourceManager.createResourceWithWait(
            KafkaBridgeTemplates.kafkaBridgeWithCors(
                suiteTestStorage.getNamespaceName(),
                suiteTestStorage.getClusterName(),
                KafkaResources.plainBootstrapAddress(suiteTestStorage.getClusterName()),
                1,
                ALLOWED_ORIGIN,
                null
            ).build()
        );

        NetworkPolicyResource.allowNetworkPolicySettingsForBridgeScraper(suiteTestStorage.getNamespaceName(), suiteTestStorage.getScraperPodName(), KafkaBridgeResources.componentName(suiteTestStorage.getClusterName()));

        KafkaBridgeHttpCors kafkaBridgeHttpCors = KafkaBridgeResource.kafkaBridgeClient().inNamespace(suiteTestStorage.getNamespaceName()).withName(suiteTestStorage.getClusterName()).get().getSpec().getHttp().getCors();
        LOGGER.info("Bridge with the following CORS settings {}", kafkaBridgeHttpCors.toString());

        bridgeUrl = KafkaBridgeResources.url(suiteTestStorage.getClusterName(), suiteTestStorage.getNamespaceName(), TestConstants.HTTP_BRIDGE_DEFAULT_PORT);
    }
}
