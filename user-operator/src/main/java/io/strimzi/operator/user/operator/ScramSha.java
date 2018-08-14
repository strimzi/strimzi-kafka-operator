/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import kafka.admin.ConfigCommand;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;

public class ScramSha {

    private static final Logger log = LogManager.getLogger(SimpleAclOperator.class.getName());

    private final ScramMechanism mechanism = ScramMechanism.SCRAM_SHA_512;

    private final String zookeeper;

    public ScramSha(String zookeeper) {
        this.zookeeper = zookeeper;
    }

    public void createUser(String username, String password, int iterations) {
        if (iterations < mechanism.minIterations()) {
            throw new RuntimeException("Given number of iterations (" + iterations + ") " +
                    "is less than minimum iterations for mechanism (" + mechanism.minIterations() + ")");
        }
        StringBuilder value = new StringBuilder(mechanism.mechanismName()).append("=[");
        if (iterations > 0) {
            value.append("iterations=").append(iterations).append(',');
        }
        value.append("password=").append(password).append(']');
        exec(asList("kafka-configs.sh",
                "--zookeeper", zookeeper,
                "--alter",
                "--entity-name", username,
                "--entity-type", "users",
                "--add-config", value.toString()));
    }

    public void deleteUser(String username) {
        exec(asList("kafka-configs.sh",
                "--zookeeper", zookeeper,
                "--alter",
                "--entity-name", username,
                "--entity-type", "users",
                "--delete-config", mechanism.mechanismName()));
    }

    private static void exec(List<String> kafkaConfigsOptions) {
        String cp = System.getProperty("java.class.path");
        File home = new File(System.getProperty("java.home"));
        List<String> arguments = new ArrayList(asList(new File(home, "bin/java").getAbsolutePath(),
                "-cp", cp,
                ConfigCommand.class.getName()));
        arguments.addAll(kafkaConfigsOptions);
        File out;
        File err;
        Process process;
        try {
            out = File.createTempFile(ScramSha.class.getName(), ".out");
            out.deleteOnExit();
            err = File.createTempFile(ScramSha.class.getName(), ".err");
            err.deleteOnExit();
            process = new ProcessBuilder(arguments)
                    .redirectOutput(out)
                    .redirectError(err)
                    //.inheritIO()
                    .start();
            log.debug("Process {} started with arguments {}", process, arguments);
            process.getOutputStream().close();
        } catch (IOException e) {
            throw new RuntimeException("Error starting subprocess " + arguments, e);
        }
        try {
            waitForProcessExit(arguments, process);
            Pattern p = Pattern.compile("Completed Updating config for entity: user-principal '.*'\\.");
            Matcher matcher = p.matcher(getFile(out));
            if (matcher.find()) {
                log.debug("Found output indicating success: {}", matcher.group());
            } else {
                throw new RuntimeException(process + " is missing expected output");
            }
        } catch (RuntimeException e) {
            log.debug("{} standard error:\n------\n{}------", process, getFile(err));
            log.debug("{} standard output:\n------\n{}------", process, getFile(out));
            throw e;
        } finally {
            if (!out.delete()) {
                log.debug("{} file could not be deleted: {}", process, out);
            }
            if (!err.delete()) {
                log.debug("{} file could not be deleted: {}", process, err);
            }
        }
    }

    private static String getFile(File out) {
        try {
            return new String(Files.readAllBytes(out.toPath()), Charset.defaultCharset());
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    private static void waitForProcessExit(List<String> arguments, Process process) {
        try {
            if (process.waitFor(30, TimeUnit.SECONDS)) {
                if (process.exitValue() == 0) {
                    log.debug("{} exit OK", process);
                } else {
                    log.warn("{} exit code {}", process, process.exitValue());
                    throw new RuntimeException("Non-zero status code from " + process);
                }
            } else {
                throw new RuntimeException("Timeout waiting for " + process);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws Exception {
        ScramSha scramSha = new ScramSha("localhost:2181");
        scramSha.createUser("tom", "password", -1);
        scramSha.createUser("tom", "password", 4096);
        scramSha.deleteUser("tom");
    }
}

