/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

import io.skodjob.testframe.LogCollector;
import io.skodjob.testframe.LogCollectorBuilder;
import io.skodjob.testframe.clients.KubeClient;
import io.skodjob.testframe.clients.cmdClient.Kubectl;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class LogCollectorUtils {
    /**
     * Method that checks existence of the folder on specified path.
     * From there, if there are no sub-dirs created - for each of the test-case run/re-run - the method returns the
     * full path containing the specified path and index (1).
     * Otherwise, it lists all the directories, filtering all the folders that are indexes, takes the last one, and returns
     * the full path containing specified path and index increased by one.
     *
     * @param rootPathToLogsForTestCase     complete path for test-class/test-class and test-case logs
     *
     * @return  full path to logs directory built from specified root path and index
     */
    public static String checkPathAndReturnFullRootPathWithIndexFolder(String rootPathToLogsForTestCase) {
        File logsForTestCase = new File(rootPathToLogsForTestCase);
        int index = 1;

        if (logsForTestCase.exists()) {
            String[] filesInLogsDir = logsForTestCase.list();

            if (filesInLogsDir != null && filesInLogsDir.length > 0) {
                index = Integer.parseInt(
                    Arrays
                        .stream(filesInLogsDir)
                        .filter(file -> {
                            try {
                                Integer.parseInt(file);
                                return true;
                            } catch (NumberFormatException e) {
                                return false;
                            }
                        })
                        .sorted()
                        .toList()
                        .get(filesInLogsDir.length - 1)
                ) + 1;
            }
        }

        return rootPathToLogsForTestCase + "/" + index;
    }

    @SuppressWarnings("deprecation") // Kafka Mirror Maker is deprecated
    public static LogCollector getDefaultLogCollector() {
        List<String> resources = new ArrayList<>(List.of(
            TestConstants.SECRET.toLowerCase(Locale.ROOT),
            TestConstants.DEPLOYMENT.toLowerCase(Locale.ROOT),
            TestConstants.CONFIG_MAP.toLowerCase(Locale.ROOT),
            Kafka.RESOURCE_SINGULAR,
            KafkaNodePool.RESOURCE_SINGULAR,
            KafkaConnect.RESOURCE_SINGULAR,
            KafkaConnector.RESOURCE_SINGULAR,
            KafkaBridge.RESOURCE_SINGULAR,
            KafkaMirrorMaker.RESOURCE_SINGULAR,
            KafkaMirrorMaker2.RESOURCE_SINGULAR,
            KafkaRebalance.RESOURCE_SINGULAR,
            KafkaTopic.RESOURCE_SINGULAR,
            KafkaUser.RESOURCE_SINGULAR
        ));

        if (Environment.isOlmInstall()) {
            resources.addAll(List.of(
                TestConstants.OPERATOR_GROUP.toLowerCase(Locale.ROOT),
                TestConstants.SUBSCRIPTION.toLowerCase(Locale.ROOT),
                TestConstants.INSTALL_PLAN.toLowerCase(Locale.ROOT),
                TestConstants.CSV.toLowerCase(Locale.ROOT)
            ));
        }

        return new LogCollectorBuilder()
            .withKubeClient(new KubeClient())
            .withKubeCmdClient(new Kubectl())
            .withRootFolderPath(Environment.TEST_LOG_DIR)
            .withNamespacedResources(resources.toArray(new String[0]))
            .build();
    }
}
