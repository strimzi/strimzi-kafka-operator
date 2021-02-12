/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.bridge;

import io.strimzi.api.kafka.model.KafkaBridgeHttpCors;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.specific.BridgeUtils;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@Tag(BRIDGE)
public class HttpBridgeCorsST extends HttpBridgeAbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeCorsST.class);
    private final String httpBridgeCorsClusterName = "http-bridge-cors-cluster-name";

    private static final String ALLOWED_ORIGIN = "https://strimzi.io";
    private static final String NOT_ALLOWED_ORIGIN = "https://evil.io";

    @ParallelTest
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
        String headers = BridgeUtils.addHeadersToString(additionalHeaders, Constants.KAFKA_BRIDGE_JSON_JSON);
        String response = cmdKubeClient().execInPod(kafkaClientsPodName, "/bin/bash", "-c", BridgeUtils.buildCurlCommand(HttpMethod.OPTIONS, url, headers, "")).out().trim();
        LOGGER.info("Response from Bridge: {}", response);

        String responseAllowHeaders = BridgeUtils.getHeaderValue("access-control-allow-headers", response);
        LOGGER.info("Checking if response from Bridge is correct");
        assertThat(response, containsString("200 OK"));
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
        response = cmdKubeClient().execInPod(kafkaClientsPodName, "/bin/bash", "-c", BridgeUtils.buildCurlCommand(HttpMethod.GET, url, headers, "")).out().trim();
        LOGGER.info("Response from Bridge: {}", response);

        assertThat(response, containsString("404"));
    }

    @ParallelTest
    void testCorsForbidden() {
        final String kafkaBridgeUser = "bridge-user-example";
        final String groupId = ClientUtils.generateRandomConsumerGroup();

        Map<String, String> additionalHeaders = new HashMap<>();
        additionalHeaders.put("Origin", NOT_ALLOWED_ORIGIN);
        additionalHeaders.put("Access-Control-Request-Method", HttpMethod.POST.toString());

        String url = bridgeUrl + "/consumers/" + groupId + "/instances/" + kafkaBridgeUser + "/subscription";
        String headers = BridgeUtils.addHeadersToString(additionalHeaders);
        String response = cmdKubeClient().execInPod(kafkaClientsPodName, "/bin/bash", "-c", BridgeUtils.buildCurlCommand(HttpMethod.OPTIONS, url, headers, "")).out().trim();
        LOGGER.info("Response from Bridge: {}", response);

        LOGGER.info("Checking if response from Bridge is correct");
        assertThat(response, containsString("403"));
        assertThat(response, containsString("CORS Rejected - Invalid origin"));

        additionalHeaders.remove("Access-Control-Request-Method", HttpMethod.POST.toString());
        headers = BridgeUtils.addHeadersToString(additionalHeaders);
        response = cmdKubeClient().execInPod(kafkaClientsPodName, "/bin/bash", "-c", BridgeUtils.buildCurlCommand(HttpMethod.POST, url, headers, "")).out().trim();
        LOGGER.info("Response from Bridge: {}", response);

        LOGGER.info("Checking if response from Bridge is correct");
        assertThat(response, containsString("403"));
        assertThat(response, containsString("CORS Rejected - Invalid origin"));
    }

    @BeforeAll
    void beforeAll(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(httpBridgeCorsClusterName, 1, 1).build());

        kafkaBridgeClientJob = kafkaBridgeClientJob.toBuilder().withBootstrapAddress(KafkaBridgeResources.serviceName(httpBridgeCorsClusterName)).build();

        String kafkaClientsName = NAMESPACE + "-shared-" + Constants.KAFKA_CLIENTS;

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
        kafkaClientsPodName = kubeClient().listPodsByPrefixInName(kafkaClientsName).get(0).getMetadata().getName();

        resourceManager.createResource(extensionContext, KafkaBridgeTemplates.kafkaBridgeWithCors(httpBridgeCorsClusterName, KafkaResources.plainBootstrapAddress(httpBridgeCorsClusterName),
            1, ALLOWED_ORIGIN, null).build());

        KafkaBridgeHttpCors kafkaBridgeHttpCors = KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(httpBridgeCorsClusterName).get().getSpec().getHttp().getCors();
        LOGGER.info("Bridge with the following CORS settings {}", kafkaBridgeHttpCors.toString());

        bridgeUrl = KafkaBridgeResources.url(httpBridgeCorsClusterName, NAMESPACE, bridgePort);
    }
}
