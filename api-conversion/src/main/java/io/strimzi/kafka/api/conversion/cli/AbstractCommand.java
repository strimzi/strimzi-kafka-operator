/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.cli;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public abstract class AbstractCommand implements Runnable {
    protected Logger log = LogManager.getLogger(getClass().getName());

    protected static final ApiVersion TO_API_VERSION = ApiVersion.V1BETA2;

    protected static final String STRIMZI_API = "kafka.strimzi.io";
    protected static final Set<String> STRIMZI_KINDS = Set.of(
            "Kafka",
            "KafkaConnect",
            "KafkaConnectS2I",
            "KafkaMirrorMaker",
            "KafkaBridge",
            "KafkaMirrorMaker2",
            "KafkaTopic",
            "KafkaUser",
            "KafkaConnector",
            "KafkaRebalance"
    );

    // Old API versions used by given resource => used to obtain the unconverted resource
    @SuppressWarnings("deprecation")
    protected final static Map<String, String> OLD_API_VERSIONS = Map.of(
            "Kafka", Kafka.V1BETA1,
            "KafkaConnect", KafkaConnect.V1BETA1,
            "KafkaConnectS2I", KafkaConnectS2I.V1BETA1,
            "KafkaMirrorMaker", KafkaMirrorMaker.V1BETA1,
            "KafkaBridge", KafkaBridge.V1ALPHA1,
            "KafkaMirrorMaker2", KafkaMirrorMaker2.V1ALPHA1,
            "KafkaTopic", KafkaTopic.V1BETA1,
            "KafkaUser", KafkaUser.V1BETA1,
            "KafkaConnector", KafkaConnector.V1ALPHA1,
            "KafkaRebalance", KafkaRebalance.V1ALPHA1
    );

    // Versioned operations are used to write the converted resource using the target API
    @SuppressWarnings({"rawtypes"})
    protected final static Map<String, BiFunction<KubernetesClient, String, MixedOperation>> VERSIONED_OPERATIONS = Map.of(
            "Kafka", Crds::kafkaOperation,
            "KafkaConnect", Crds::kafkaConnectOperation,
            "KafkaConnectS2I", Crds::kafkaConnectS2iOperation,
            "KafkaMirrorMaker", Crds::mirrorMakerOperation,
            "KafkaBridge", Crds::kafkaBridgeOperation,
            "KafkaMirrorMaker2", Crds::kafkaMirrorMaker2Operation,
            "KafkaTopic", Crds::topicOperation,
            "KafkaUser", Crds::kafkaUserOperation,
            "KafkaConnector", Crds::kafkaConnectorOperation,
            "KafkaRebalance", Crds::kafkaRebalanceOperation
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
     * @param value     Object which should be printed
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
