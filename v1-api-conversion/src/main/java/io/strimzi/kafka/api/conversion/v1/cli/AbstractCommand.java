/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.cli;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.common.Constants;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.Map;
import java.util.Set;

/**
 * Base class for all the commands.
 */
public abstract class AbstractCommand implements Runnable {
    protected Logger log = LogManager.getLogger(getClass().getName());

    protected static final ApiVersion FROM_API_VERSION = ApiVersion.V1BETA2;
    protected static final ApiVersion TO_API_VERSION = ApiVersion.V1;
    protected static final Set<String> STRIMZI_KINDS = Set.of(
            "Kafka",
            "KafkaConnect",
            "KafkaBridge",
            "KafkaMirrorMaker2",
            "KafkaTopic",
            "KafkaUser",
            "KafkaConnector",
            "KafkaRebalance",
            "KafkaNodePool",
            "StrimziPodSet"
    );

    protected static final Map<String, String> STRIMZI_GROUPS = Map.of(
            "Kafka", Constants.RESOURCE_GROUP_NAME,
            "KafkaConnect", Constants.RESOURCE_GROUP_NAME,
            "KafkaBridge", Constants.RESOURCE_GROUP_NAME,
            "KafkaMirrorMaker2", Constants.RESOURCE_GROUP_NAME,
            "KafkaTopic", Constants.RESOURCE_GROUP_NAME,
            "KafkaUser", Constants.RESOURCE_GROUP_NAME,
            "KafkaConnector", Constants.RESOURCE_GROUP_NAME,
            "KafkaRebalance", Constants.RESOURCE_GROUP_NAME,
            "KafkaNodePool", Constants.RESOURCE_GROUP_NAME,
            "StrimziPodSet", Constants.RESOURCE_CORE_GROUP_NAME
    );

    // Mapping between kinds and the CRD names used to get the right CRD from the Kubernetes API
    @SuppressWarnings("SpellCheckingInspection")
    protected static final Map<String, String> CRD_NAMES = Map.of(
            "Kafka", "kafkas." + Constants.RESOURCE_GROUP_NAME,
            "KafkaConnect", "kafkaconnects." + Constants.RESOURCE_GROUP_NAME,
            "KafkaBridge", "kafkabridges." + Constants.RESOURCE_GROUP_NAME,
            "KafkaMirrorMaker2", "kafkamirrormaker2s." + Constants.RESOURCE_GROUP_NAME,
            "KafkaTopic", "kafkatopics." + Constants.RESOURCE_GROUP_NAME,
            "KafkaUser", "kafkausers." + Constants.RESOURCE_GROUP_NAME,
            "KafkaConnector", "kafkaconnectors." + Constants.RESOURCE_GROUP_NAME,
            "KafkaRebalance", "kafkarebalances." + Constants.RESOURCE_GROUP_NAME,
            "KafkaNodePool", "kafkanodepools." + Constants.RESOURCE_GROUP_NAME,
            "StrimziPodSet", "strimzipodsets." + Constants.RESOURCE_CORE_GROUP_NAME
    );

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-d", "--debug"}, description = "Runs the tool in debug mode")
    boolean debug;

    @CommandLine.Option(names = {"-ll", "--log-level"}, description = "Sets the log level to enable logging")
    Level level;

    /**
     * Constructor
     */
    public AbstractCommand() { }

    /**
     * Prints the value to the standard output using PicoCLI. It is important to use this instead of regular
     * System.out.println to be able to easily capture the output in tests.
     *
     * @param value     Object that should be printed
     */
    protected void println(Object value) {
        if (level != null) {
            log.log(level, String.valueOf(value));
        } else {
            spec.commandLine().getOut().println(value);
        }
    }

    /**
     * Prints empty line to the standard output using PicoCLI. It is important to use this instead of regular
     * System.out.println to be able to easily capture the output in tests.
     */
    protected void println() {
        if (level == null) {
            spec.commandLine().getOut().println();
        }
    }
}
