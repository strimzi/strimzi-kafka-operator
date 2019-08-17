/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.clients.api;

import io.strimzi.systemtest.executor.Executor;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

import static io.strimzi.test.BaseITST.kubeClient;

/**
 * Class represent verifiable kafka client which keeps common features of kafka clients
 */
public class VerifiableClient {

    private static final Logger LOGGER = LogManager.getLogger(VerifiableClient.class);
    protected ArrayList<ClientArgument> allowedArguments = new ArrayList<>();
    private final Object lock = new Object();
    private JsonArray messages = new JsonArray();
    private ArrayList<String> arguments = new ArrayList<>();
    private String executable;
    private ClientType clientType;
    private Executor executor;
    private String podName;
    private String podNamespace;

    /**
     * Constructor of verifiable kafka client
     *
     * @param clientType type of kafka client
     */
    public VerifiableClient(ClientType clientType) {
        this.setAllowedArguments(clientType);
        this.podName = kubeClient().listPodsByPrefixInName("my-cluster-kafka-clients-").get(0).getMetadata().getName();
        this.podNamespace = kubeClient().getNamespace();
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
     * Get all kafka client arguments.
     *
     * @return The kafka client arguments.
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
     * Run clients
     *
     * @param timeout kill timeout in ms
     * @return true if command end with exit code 0
     */
    private boolean runClient(int timeout, boolean logToOutput) {
        messages.clear();
        try {
            executor = new Executor();
            int ret = executor.execute(prepareCommand(), timeout);
            synchronized (lock) {
                LOGGER.info("{} {} Return code - {}", this.getClass().getName(), clientType,  ret);
                if (logToOutput) {
                    LOGGER.info("{} {} stdout : {}", this.getClass().getName(), clientType, executor.getStdOut());
                    if (!executor.getStdErr().isEmpty()) {
                        LOGGER.error("{} {} stderr : {}", this.getClass().getName(), clientType, executor.getStdErr());
                    }
                    if (ret == 0) {
                        parseToJson(executor.getStdOut());
                    }
                }
            }
            return ret == 0;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    /**
     * Method for parse string output to json array of messages
     *
     * @param data string data output
     */
    private void parseToJson(String data) {
        if (data != null) {
            for (String line : data.split(System.getProperty("line.separator"))) {
                if (!Objects.equals(line, "") && !line.trim().isEmpty()) {
                    try {
                        messages.add(new JsonObject(line));
                    } catch (Exception ignored) {
                        LOGGER.warn("{} - Failed to parse client output '{}' as JSON", clientType, line);
                    }
                }
            }
        }
    }

    /**
     * Merge command and arguments
     *
     * @return merged array of command and args
     */
    private ArrayList<String> prepareCommand() {
        ArrayList<String> command = new ArrayList<>(arguments);
        ArrayList<String> executableCommand = new ArrayList<>();
        // TODO: create divider oc and kubectl
        executableCommand.addAll(Arrays.asList("oc", "exec", podName, "-n", podNamespace, "--"));
        executableCommand.add(executable);
        executableCommand.addAll(command);
        return executableCommand;
    }

    /**
     * Run client in sync mode
     *
     * @return exit status of client
     */
    public boolean run() {
        return runClient(60000, true);
    }

    /**
     * Method for stop client
     */
    public void stop() {
        try {
            executor.stop();
        } catch (Exception ex) {
            LOGGER.warn("Client stop raise exception: " + ex.getMessage());
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
                break;
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
                allowedArguments.add(ClientArgument.GROUP_INSTANCE_ID);
                break;
            default:
                throw new IllegalArgumentException("Unexpected client type!");
        }
    }
}
