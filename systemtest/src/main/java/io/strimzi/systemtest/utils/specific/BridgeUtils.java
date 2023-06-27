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

public class BridgeUtils {

    private static final Logger LOGGER = LogManager.getLogger(HttpUtils.class);

    private BridgeUtils() { }

    public static String buildCurlCommand(HttpMethod httpMethod, String url, String headers, String data) {
        String command = "curl -X " + httpMethod.toString() + " -D - " + url + " " + headers;

        if (!data.isEmpty() && (httpMethod == HttpMethod.POST || httpMethod == HttpMethod.PUT)) {
            command += " -d " + "'" + data + "'";
        }

        return command;
    }

    public static String addHeadersToString(Map<String, String> additionalHeaders) {
        return addHeadersToString(additionalHeaders, "",  "");
    }

    public static String addHeadersToString(Map<String, String> additionalHeaders, String contentType) {
        return addHeadersToString(additionalHeaders, contentType,  "");
    }

    public static String addHeadersToString(Map<String, String> additionalHeaders,  String contentType, String content) {
        StringBuilder headerString = new StringBuilder();

        if (!content.isEmpty()) {
            headerString.append(" -H 'Content-length: ").append(content.length()).append("'");
        }

        if (!contentType.isEmpty()) {
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
     * Returns Strimzi KafkaBridge version which is associated with Strimzi Kafka Operator.
     * The value is parsed from {@code /bridge.version} classpath resource and return it as a string.
     * @return bridge version
     */
    public static String getBridgeVersion() {
        InputStream bridgeVersionInputStream = BridgeUtils.class.getResourceAsStream("/bridge.version");
        return TestUtils.readResource(bridgeVersionInputStream).replace("\n", "");
    }
}
