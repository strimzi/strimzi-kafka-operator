/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

/**
 * Class represent verifiable kafka client which keeps common features of kafka clients
 */
public class VerifiableClient {

    private static final Logger LOGGER = LogManager.getLogger(VerifiableClient.class);
    private List<ClientArgument> allowedArguments = new ArrayList<>();
    private final Object lock = new Object();
    private List<String> messages = new ArrayList<>();
    private List<String> arguments = new ArrayList<>();
    private String executable;
    private ClientType clientType;
    private Exec executor;
    private String podName;
    private String podNamespace;

    /**
     * Constructor of verifiable kafka client
     *
     * @param clientType type of kafka client
     */
    public VerifiableClient(ClientType clientType, String podName, String podNamespace) {
        this.setAllowedArguments(clientType);
        this.clientType = clientType;
        this.podName = podName;
        this.podNamespace = podNamespace;
        this.executable = ClientType.getCommand(clientType);
    }

    /**
     * Get of messages
     *
     * @return Json array of messages;
     */
    public List<String> getMessages() {
        return messages;
    }

    /**
     * Set arguments of kafka client
     *
     * @param args string array of arguments
     */
    public void setArguments(ClientArgumentMap args) {
        arguments.clear();
        String argument;
        for (ClientArgument arg : args.getArguments()) {
            if (validateArgument(arg)) {
                for (String value : args.getValues(arg)) {
                    if (arg.equals(ClientArgument.USER)) {
                        argument = String.format("%s=%s", arg.command(), value);
                        arguments.add(argument);
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
     * @param timeoutMs kill timeout in ms
     * @return true if command end with exit code 0
     */
    private boolean runClient(long timeoutMs, boolean logToOutput) {
        messages.clear();
        try {
            executor = new Exec();
            int ret = executor.execute(null, prepareCommand(), timeoutMs);
            synchronized (lock) {
                LOGGER.info("{} {} Return code - {}", this.getClass().getSimpleName(), clientType,  ret);
                if (logToOutput) {
                    LOGGER.info("{} {} stdout : {}", this.getClass().getSimpleName(), clientType, executor.out());
                    if (ret == 0) {
                        parseToList(executor.out());
                    } else if (!executor.err().isEmpty()) {
                        LOGGER.error("{} {} stderr : {}", this.getClass().getSimpleName(), clientType, executor.err());
                    }
                }
            }
            return ret == 0;
        } catch (InterruptedException | ExecutionException | IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Method for parse string output to List<string>
     *
     * @param data string data output
     */
    private void parseToList(String data) {
        if (data != null) {
            for (String line : data.split(System.getProperty("line.separator"))) {
                if (!Objects.equals(line, "") && !line.trim().isEmpty()) {
                    try {
                        messages.add(line);
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
        executableCommand.addAll(Arrays.asList(cmdKubeClient().toString(), "exec", podName, "-n", podNamespace, "--"));
        executableCommand.add(executable);
        executableCommand.addAll(command);
        return executableCommand;
    }

    /**
     * Run client in sync mode
     *
     * @return exit status of client
     */
    public boolean run(long timeoutMs) {
        return runClient(timeoutMs, true);
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
