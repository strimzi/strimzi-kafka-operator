/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.HttpUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;

public class BridgeUtils {

    private static final Logger LOGGER = LogManager.getLogger(HttpUtils.class);

    public static final Pattern ALL_BEFORE_JSON_PATTERN = Pattern.compile("(.*\\s)\\{", Pattern.DOTALL);
    private static final Pattern ALL_BEFORE_JSON_ARRAY_PATTERN = Pattern.compile("(.*\\s)\\[", Pattern.DOTALL);

    public static final String DEFAULT_BRIDGE_HOST = "localhost:" + Constants.HTTP_BRIDGE_DEFAULT_PORT;
    private static String url = "";
    private static String headers = "";
    private static String response = "";

    private BridgeUtils() { }

    public static JsonObject generateHttpMessages(int messageCount) {
        LOGGER.info("Creating {} records for KafkaBridge", messageCount);
        JsonArray records = new JsonArray();
        JsonObject json = new JsonObject();
        for (int i = 0; i < messageCount; i++) {
            json.put("value", "msg_" + i);
            records.add(json);
        }
        JsonObject root = new JsonObject();
        root.put("records", records);
        return root;
    }

    public static JsonObject sendMessagesHttpRequest(JsonObject records, String topicName, String podName) {
        LOGGER.info("Sending records to KafkaBridge");

        url = DEFAULT_BRIDGE_HOST + "/topics/" + topicName;
        headers = addHeadersToString(Constants.KAFKA_BRIDGE_JSON_JSON, records.toString());
        response = executeCurlCommand(HttpMethod.POST, podName, records.toString(), url, headers);

        Matcher matcher = ALL_BEFORE_JSON_PATTERN.matcher(response);
        JsonObject jsonResponse = new JsonObject(matcher.replaceFirst("{"));

        if (response.contains("200 OK")) {
            LOGGER.debug("Server accepted post");
        } else {
            throw new RuntimeException("Server didn't accept post: " + response);
        }

        return jsonResponse;
    }

    public static JsonArray receiveMessagesHttpRequest(String podName, String groupID, String name) {
        LOGGER.info("Trying to receive messages");
        JsonArray jsonResponse = receiveMessages(podName, groupID, name);
        if (jsonResponse.size() == 0) {
            LOGGER.info("Received 0 messages, trying again after subscribing to offset");
            jsonResponse = receiveMessages(podName, groupID, name);
        }

        return jsonResponse;
    }

    public static JsonArray receiveMessages(String podName, String groupID, String name) {
        LOGGER.info("Receiving records from KafkaBridge");

        url = DEFAULT_BRIDGE_HOST + "/consumers/" + groupID + "/instances/" + name + "/records?timeout=" + 1000;
        headers = addHeadersToString(Collections.singletonMap("Accept", Constants.KAFKA_BRIDGE_JSON_JSON));
        response = executeCurlCommand(HttpMethod.GET, podName, "", url, headers);

        Matcher matcher = ALL_BEFORE_JSON_ARRAY_PATTERN.matcher(response);
        JsonArray jsonResponse = new JsonArray(matcher.replaceFirst("["));

        if (response.contains("200 OK")) {
            if (jsonResponse.size() > 0) {
                for (int i = 0; i < jsonResponse.size(); i++) {
                    JsonObject jsonObject = jsonResponse.getJsonObject(i);
                    LOGGER.info("JsonResponse: {}", jsonObject.toString());
                    String kafkaTopic = jsonObject.getString("topic");
                    int kafkaPartition = jsonObject.getInteger("partition");
                    String key = jsonObject.getString("key");
                    Object value = jsonObject.getValue("value");
                    long offset = jsonObject.getLong("offset");
                    LOGGER.debug("Received msg: topic:{} partition:{} key:{} value:{} offset{}", kafkaTopic, kafkaPartition, key, value, offset);
                }
                LOGGER.info("Received {} messages from KafkaBridge", jsonResponse.size());
            } else {
                LOGGER.warn("Received body 0 messages: {}", jsonResponse);
            }
        } else {
            LOGGER.info("Cannot consume any messages: {}", jsonResponse);
        }

        return jsonResponse;
    }

    public static boolean subscribeHttpConsumer(String podName, JsonObject topics, String groupId, String name) {
        return subscribeHttpConsumer(podName, topics, groupId, name, Collections.emptyMap());
    }

    public static boolean subscribeHttpConsumer(String podName, JsonObject topics, String groupId, String name, Map<String, String> additionalHeaders) {
        url = DEFAULT_BRIDGE_HOST + "/consumers/" + groupId + "/instances/" + name + "/subscription";
        headers = addHeadersToString(additionalHeaders, Constants.KAFKA_BRIDGE_JSON, topics.toString());
        response = executeCurlCommand(HttpMethod.POST, podName, topics.toString(), url, headers);

        if (response.contains("204")) {
            LOGGER.info("Consumer subscribed");
            return true;
        } else {
            throw new RuntimeException("Cannot subscribe consumer " + response);
        }
    }

    public static String createHttpConsumer(String podName, JsonObject config, String groupId) {
        return createHttpConsumer(podName, config, groupId, Collections.emptyMap());
    }

    public static String createHttpConsumer(String podName, JsonObject config, String groupId, Map<String, String> additionalHeaders) {
        LOGGER.info("Creating consumer");

        url = DEFAULT_BRIDGE_HOST + "/consumers/" + groupId;
        headers = addHeadersToString(additionalHeaders, Constants.KAFKA_BRIDGE_JSON, config.toString());
        response = executeCurlCommand(HttpMethod.POST, podName, config.toString(), url, headers);

        Matcher matcher = ALL_BEFORE_JSON_PATTERN.matcher(response);
        JsonObject jsonResponse = new JsonObject(matcher.replaceFirst("{"));

        if (response.contains("200 OK")) {
            String consumerInstanceId = jsonResponse.getString("instance_id");
            String consumerBaseUri = jsonResponse.getString("base_uri");
            LOGGER.debug("ConsumerInstanceId: {}", consumerInstanceId);
            LOGGER.debug("ConsumerBaseUri: {}", consumerBaseUri);
        } else {
            throw new RuntimeException("Cannot create consumer " + response);
        }

        return response;
    }

    public static boolean deleteConsumer(String podName, String groupId, String name) {
        LOGGER.info("Deleting consumer");

        url = DEFAULT_BRIDGE_HOST + "/consumers/" + groupId + "/instances/" + name;
        headers = "";
        response = executeCurlCommand(HttpMethod.DELETE, podName, url, headers);

        if (response.contains("204 No Content")) {
            return true;
        } else {
            throw new RuntimeException("Cannot delete consumer " + response);
        }
    }

    public static String getCurlCommand(HttpMethod httpMethod, String url, String headers, String data) {
        String command = "curl -X " + httpMethod.toString() + " -D - " + url + " " + headers;

        if (!data.equals("")) {
            command += " -d " + "'" + data + "'";
        }

        return command;
    }

    public static String executeCurlCommand(HttpMethod httpMethod, String podName, String url, String headers) {
        return executeCurlCommand(httpMethod, podName, "", url, headers);
    }

    public static String executeCurlCommand(HttpMethod httpMethod, String podName, String data, String url, String headers) {
        return cmdKubeClient().execInPod(podName, "/bin/bash", "-c", getCurlCommand(httpMethod, url, headers, data)).out().trim();
    }

    public static String addHeadersToString(String contentType, String content) {
        return addHeadersToString(Collections.emptyMap(), contentType,  content);
    }

    public static String addHeadersToString(Map<String, String> additionalHeaders) {
        return addHeadersToString(additionalHeaders, "",  "");
    }

    public static String addHeadersToString(Map<String, String> additionalHeaders,  String contentType, String content) {
        StringBuilder headerString = new StringBuilder();

        if (!content.equals("")) {
            headerString.append(" -H 'Content-length: ").append(content.length()).append("'");
        }

        if (!contentType.equals("")) {
            headerString.append(" -H 'Content-type: ").append(contentType).append("'");
        }

        for (Map.Entry<String, String> header : additionalHeaders.entrySet()) {
            LOGGER.info("Adding header {} -> {}", header.getKey(), header.getValue());
            headerString.append(" -H '").append(header.getKey()).append(": ").append(header.getValue()).append("'");
        }

        return headerString.toString();
    }

    public static String getHeaderValue(String expectedHeader, String response) {
        Pattern headerPattern = Pattern.compile(expectedHeader + ": \\s*([^\\n\\r]*)");
        Matcher matcher = headerPattern.matcher(response);

        if (matcher.find()) {
            return matcher.group(1);
        } else {
            LOGGER.error("Cannot find value for header: {}", expectedHeader);
            return "";
        }
    }

    /**
     * Returns Strimzi Kafka Bridge version which is associated with Strimzi Kafka Operator.
     * The value is parsed from {@code /bridge.version} classpath resource and return it as a string.
     * @return bridge version
     */
    public static String getBridgeVersion() {
        InputStream bridgeVersionInputStream = BridgeUtils.class.getResourceAsStream("/bridge.version");
        return TestUtils.readResource(bridgeVersionInputStream).replace("\n", "");
    }
}
