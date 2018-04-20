package io.strimzi.controller.cluster.model;

import io.vertx.core.json.JsonObject;

import java.util.List;

import static java.util.Arrays.asList;

public class KafkaConfiguration extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_OPTIONS;

    static {
        FORBIDDEN_OPTIONS = asList(
                "listeners",
                "advertised.listeners",
                "broker.id",
                "listener.",
                "inter.broker.listener.name",
                "sasl.",
                "ssl.",
                "log.dirs",
                "zookeeper.connect",
                "authorizer.",
                "super.user");
    }

    public KafkaConfiguration(String configurationFile) {
        super(configurationFile, FORBIDDEN_OPTIONS);
    }

    public KafkaConfiguration(JsonObject jsonOptions) {
        super(jsonOptions, FORBIDDEN_OPTIONS);
    }
}
