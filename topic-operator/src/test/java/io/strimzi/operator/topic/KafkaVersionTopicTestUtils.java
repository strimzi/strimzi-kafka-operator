/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaVersionTopicTestUtils {

    public static final String KAFKA_VERSIONS_RESOURCE = "kafka-versions.yaml";

    public static String kafkaVersion() {
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br
                     = new BufferedReader(new InputStreamReader(KafkaVersionTopicTestUtils.class.getResourceAsStream("/" + KAFKA_VERSIONS_RESOURCE)))) {
            String line;
            while (true) {
                if (!((line = br.readLine()) != null))  {
                    break;
                } else {
                    resultStringBuilder.append(line).append("\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Pattern p = Pattern.compile("- version: (\\d\\.\\d\\.\\d)");
        Matcher m = p.matcher(resultStringBuilder.toString());
        String version = null;
        // find the last occurrence
        while (m.find()) {
            version = m.group(1);
        }

        return version;
    }
}
