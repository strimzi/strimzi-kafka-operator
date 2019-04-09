/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

import io.vertx.core.json.JsonArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

/**
 * Class represent verifiable kafka client which keeps common features of kafka clients
 */
public class VerifiableClient {
    private static final Logger LOGGER = LogManager.getLogger(VerifiableClient.class);
    protected ArrayList<ClientArgument> allowedArguments = new ArrayList<>();
    private ClientType clientType;
    private JsonArray messages = new JsonArray();
    private ArrayList<String> arguments = new ArrayList<>();
    private String executable;

    /**
     * Constructor of verifiable kafka client
     *
     * @param clientType type of kafka client
     */
    public VerifiableClient(ClientType clientType) {
        this.clientType = clientType;
        this.setAllowedArguments(clientType);
        this.executable = ClientType.getCommand(clientType);
    }

    /**
     * Get of messages
     *
     * @return Json array of messages;
     */
    public JsonArray getMessages() {
        return messages;
    }

    /**
     * Get all kafka client arguments
     *
     * @return
     */
    public ArrayList<String> getArguments() {
        return arguments;
    }

    public String getExecutable() {
        return this.executable;
    }

    /**
     * Set arguments of kafka client
     *
     * @param args string array of arguments
     */
    public void setArguments(ClientArgumentMap args) {
        arguments.clear();
        String test;
        for (ClientArgument arg : args.getArguments()) {
            if (validateArgument(arg)) {
                for (String value : args.getValues(arg)) {
                    if (arg.equals(ClientArgument.USER)) {
                        test = String.format("%s=%s", arg.command(), value);
                        arguments.add(test);
                    } else {
                        arguments.add(arg.command());
                        if (!value.isEmpty()) {
                            arguments.add(value);
                        }
                    }
                }
            } else {
                LOGGER.warn(String.format("Argument '%s' is not allowed for '%s'",
                        arg.command(),
                        this.getClass().getSimpleName()));
            }
        }
    }

    /**
     * Validates that kafka client support this arg
     *
     * @param arg argument to validate
     * @return true if argument is supported
     */
    private boolean validateArgument(ClientArgument arg) {
        return this.allowedArguments.contains(arg);
    }

    /**
     * Set allowed args for verifiable clients
     * @param clientType client type
     */
    protected void setAllowedArguments(ClientType clientType) {
        switch (clientType) {
            case CLI_KAFKA_VERIFIABLE_PRODUCER:
                allowedArguments.add(ClientArgument.TOPIC);
                allowedArguments.add(ClientArgument.BROKER_LIST);
                allowedArguments.add(ClientArgument.MAX_MESSAGES);
                allowedArguments.add(ClientArgument.THROUGHPUT);
                allowedArguments.add(ClientArgument.ACKS);
                allowedArguments.add(ClientArgument.PRODUCER_CONFIG);
                allowedArguments.add(ClientArgument.MESSAGE_CREATE_TIME);
                allowedArguments.add(ClientArgument.VALUE_PREFIX);
                allowedArguments.add(ClientArgument.REPEATING_KEYS);
                allowedArguments.add(ClientArgument.USER);
            case CLI_KAFKA_VERIFIABLE_CONSUMER:
                allowedArguments.add(ClientArgument.BROKER_LIST);
                allowedArguments.add(ClientArgument.TOPIC);
                allowedArguments.add(ClientArgument.GROUP_ID);
                allowedArguments.add(ClientArgument.MAX_MESSAGES);
                allowedArguments.add(ClientArgument.SESSION_TIMEOUT);
                allowedArguments.add(ClientArgument.VERBOSE);
                allowedArguments.add(ClientArgument.ENABLE_AUTOCOMMIT);
                allowedArguments.add(ClientArgument.RESET_POLICY);
                allowedArguments.add(ClientArgument.ASSIGMENT_STRATEGY);
                allowedArguments.add(ClientArgument.CONSUMER_CONFIG);
                allowedArguments.add(ClientArgument.USER);
                break;
            default:
                throw new IllegalArgumentException("Unexpected client type!");
        }
    }
}
