/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.cli;

import io.strimzi.api.annotations.ApiVersion;
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
    protected static final String STRIMZI_API = "kafka.strimzi.io";
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
            "Kafka", "kafka.strimzi.io",
            "KafkaConnect", "kafka.strimzi.io",
            "KafkaBridge", "kafka.strimzi.io",
            "KafkaMirrorMaker2", "kafka.strimzi.io",
            "KafkaTopic", "kafka.strimzi.io",
            "KafkaUser", "kafka.strimzi.io",
            "KafkaConnector", "kafka.strimzi.io",
            "KafkaRebalance", "kafka.strimzi.io",
            "KafkaNodePool", "kafka.strimzi.io",
            "StrimziPodSet", "core.strimzi.io"
    );

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-d", "--debug"}, description = "Runs the tool in debug mode")
    boolean debug;

    @CommandLine.Option(names = {"-ll", "--log-level"}, description = "Sets the log level to enable logging")
    Level level;

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
