/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.utils.HttpUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;

public class BridgeUtils {

    private static final Logger LOGGER = LogManager.getLogger(HttpUtils.class);

    private BridgeUtils() { }

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
