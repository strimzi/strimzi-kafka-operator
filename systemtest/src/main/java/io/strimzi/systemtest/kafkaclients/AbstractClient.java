/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

import io.strimzi.test.k8s.Exec;
import io.strimzi.test.k8s.ExecResult;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;

/**
 * Class represent abstract kafka client which keeps common features of kafka clients
 */
public abstract class AbstractClient {
    private static final Logger LOGGER = LogManager.getLogger(AbstractClient.class);
    private final Object lock = new Object();
    private static final int DEFAULT_ASYNC_TIMEOUT = 120000;
    private static final int DEFAULT_SYNC_TIMEOUT = 60000;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss-SSSS");
    protected ArrayList<ClientArgument> allowedArgs = new ArrayList<>();
    private Exec executor;
    private ClientType clientType;
    private JsonArray messages = new JsonArray();
    private ArrayList<String> arguments = new ArrayList<>();
    private Path logPath;
    private String executable;

    /**
     * Constructor of abstract kafka client
     *
     * @param clientType type of kafka client
     */
    public AbstractClient(ClientType clientType) {
        this.clientType = clientType;
        this.fillAllowedArgs();
        this.executable = ClientType.getCommand(clientType);
    }

    /**
     * Constructor of abstract kafka client
     *
     * @param clientType type of kafka client
     * @param logPath    path where logs will be stored
     */
    public AbstractClient(ClientType clientType, Path logPath) {
        this.clientType = clientType;
        this.logPath = Paths.get(logPath.toString(), clientType.toString() + "_" + dateFormat.format(new Date()));
        this.fillAllowedArgs();
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
     * Return type of kafka client
     *
     * @return type of kafka client
     */
    public ClientType getClientType() {
        return clientType;
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
     * @param clientType
     */
    public void setClientType(ClientType clientType) {
        this.clientType = clientType;
        this.executable = ClientType.getCommand(clientType);
    }

    public String getStdOut() {
        return executor.getStdOut();
    }

    public String getStdErr() {
        return executor.getStdErr();
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
        return this.allowedArgs.contains(arg);
    }

    /**
     * Fill with clients supported args
     */
    protected abstract void fillAllowedArgs();

    /**
     * Run clients
     *
     * @param timeout kill timeout in ms
     * @return true if command end with exit code 0
     */
    private boolean runClient(int timeout, boolean logToOutput) {
        messages.clear();
        try {
            executor = new Exec();
            ExecResult result = executor.exec(null, prepareCommand(), timeout, false);
            synchronized (lock) {
                LOGGER.info("Return code - " + result.exitStatus());
                if (logToOutput) {
                    if (result.exitStatus() == 0) {
                        LOGGER.info(executor.getStdOut());
                        parseToJson(executor.getStdOut());
                    } else {
                        LOGGER.error(executor.getStdErr());
                    }
                }
            }
            return result.exitStatus() == 0;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
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
        executableCommand.add(executable);
        executableCommand.addAll(command);
        return executableCommand;
    }

    /**
     * Run kafka client async
     *
     * @return future of exit status of kafka client
     */
    public Future<Boolean> runAsync() {
        return runAsync(true);
    }

    /**
     * Run kafka client in sync mode
     *
     * @return exit status of webClient
     */
    public boolean run() {
        return runClient(DEFAULT_SYNC_TIMEOUT, true);
    }

    /**
     * Run kafka client async
     *
     * @param logToOutput enable logging of stdOut and stdErr on output
     * @return future of exit status of kafka client
     */
    public Future<Boolean> runAsync(boolean logToOutput) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return runClient(DEFAULT_ASYNC_TIMEOUT, logToOutput);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, runnable -> new Thread(runnable).start());
    }

    /**
     * Run kafka client async
     *
     * @param logToOutput           enable logging of stdOut and stdErr on output
     * @param timeoutInMilliseconds timeout to kill process
     * @return future of exit status of kafka client
     */
    public boolean run(int timeoutInMilliseconds, boolean logToOutput) {
        return runClient(timeoutInMilliseconds, logToOutput);
    }

    /**
     * Run kafka client in sync mode
     *
     * @param logToOutput enable logging of stdOut and stdErr on output
     * @return exit status of kafka client
     */
    public boolean run(boolean logToOutput) {
        return runClient(DEFAULT_SYNC_TIMEOUT, logToOutput);
    }

    /**
     * Run kafka client in sync mode with timeout
     *
     * @param timeout kill timeout in ms
     * @return exit status of kafka client
     */
    public boolean run(int timeout) {
        return runClient(timeout, true);
    }

    /**
     * Method for stop kafka client
     */
    public void stop() {
        try {
            executor.stop();
        } catch (Exception ex) {
            LOGGER.warn("Client stop raise exception: " + ex.getMessage());
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
                    }
                }
            }
        }
    }
}
