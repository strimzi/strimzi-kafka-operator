/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.process.ProcessHelper;
import kafka.admin.ConfigCommand;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;

public class ScramShaCredentials {

    private static final Logger log = LogManager.getLogger(SimpleAclOperator.class.getName());

    private final ScramMechanism mechanism = ScramMechanism.SCRAM_SHA_512;

    private final String zookeeper;

    public ScramShaCredentials(String zookeeper) {
        this.zookeeper = zookeeper;
    }

    /**
     * Create or update the SCRAM-SHA credentials for the given user.
     */
    public void createOrUpdate(String username, String password, int iterations) {
        if (iterations < mechanism.minIterations()) {
            throw new RuntimeException("Given number of iterations (" + iterations + ") " +
                    "is less than minimum iterations for mechanism (" + mechanism.minIterations() + ")");
        }
        StringBuilder value = new StringBuilder(mechanism.mechanismName()).append("=[");
        if (iterations > 0) {
            value.append("iterations=").append(iterations).append(',');
        }
        value.append("password=").append(password).append(']');
        try (ProcessHelper.ProcessResult pr = exec(asList(
                    "--zookeeper", zookeeper,
                    "--alter",
                    "--entity-name", username,
                    "--entity-type", "users",
                    "--add-config", value.toString()))) {
            Pattern compile = Pattern.compile("Completed Updating config for entity: user-principal '.*'\\.");
            if (!matchResult(pr, pr.standardOutput(), 0, compile)) {
                throw unexpectedOutput(pr);
            }
        }
    }

    /**
     * Delete the SCRAM-SHA credentials for the given user.
     * It is not an error if the user doesn't exist, or doesn't currently have any SCRAM-SHA credentials.
     */
    public void delete(String username) {
        try (ProcessHelper.ProcessResult pr = exec(asList(
                "--zookeeper", zookeeper,
                "--alter",
                "--entity-name", username,
                "--entity-type", "users",
                "--delete-config", mechanism.mechanismName()))) {
            if (!matchResult(pr, pr.standardOutput(), 0,
                    Pattern.compile("Completed Updating config for entity: user-principal '.*'\\."))
                    && !matchResult(pr, pr.standardError(), 1,
                    Pattern.compile(Pattern.quote("Invalid config(s): " + mechanism.mechanismName())))) {
                throw unexpectedOutput(pr);
            }
        }
    }

    /**
     * Determine whether the given user has SCRAM-SHA credentials.
     */
    public boolean exists(String username) {
        try (ProcessHelper.ProcessResult pr = exec(asList("kafka-configs.sh",
            "--zookeeper", zookeeper,
            "--describe",
            "--entity-name", username,
            "--entity-type", "users"))) {
            if (matchResult(pr, pr.standardOutput(), 0,
                    Pattern.compile("Configs for user-principal '.*?' are .*" + mechanism.mechanismName() + "=salt=[a-zA-Z0-9=]+,stored_key=([a-zA-Z0-9/+=]+),server_key=([a-zA-Z0-9/+=]+),iterations=[0-9]+"))) {
                return true;
            } else if (matchResult(pr, pr.standardOutput(), 0,
                    Pattern.compile("Configs for user-principal '.*?' are .*(?!" + mechanism.mechanismName() + "=salt=[a-zA-Z0-9=]+,stored_key=([a-zA-Z0-9/+=]+),server_key=([a-zA-Z0-9/+=]+),iterations=[0-9]+)"))) {
                return false;
            } else {
                throw unexpectedOutput(pr);
            }
        }
    }

    /**
     * List users with SCRAM-SHA credentials
     */
    public List<String> list() {
        List<String> result = new ArrayList<>();
        try (ProcessHelper.ProcessResult pr = exec(asList("kafka-configs.sh",
                "--zookeeper", zookeeper,
                "--describe",
                "--entity-type", "users"))) {
            if (pr.exitCode() == 0) {
                Pattern pattern = Pattern.compile("Configs for user-principal '(.*?)' are .*" + mechanism.mechanismName() + "=salt=[a-zA-Z0-9=]+,stored_key=([a-zA-Z0-9/+=]+),server_key=([a-zA-Z0-9/+=]+),iterations=[0-9]+");
                try {
                    try (FileChannel channel = new FileInputStream(pr.standardOutput()).getChannel()) {
                        MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, (int) channel.size());
                        CharBuffer cs = Charset.defaultCharset().newDecoder().decode(byteBuffer);
                        Matcher m = pattern.matcher(cs);
                        while (m.find()) {
                            result.add(m.group(1));
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return result;
    }

    private RuntimeException unexpectedOutput(ProcessHelper.ProcessResult pr) {
        log.debug("{} standard output:\n~~~\n{}\n~~~", pr, getFile(pr.standardOutput()));
        log.debug("{} standard error:\n~~~\n{}\n~~~", pr, getFile(pr.standardError()));
        return new RuntimeException(pr + " exited with code " + pr.exitCode() + " and is missing expected output");
    }

    boolean matchResult(ProcessHelper.ProcessResult pr, File file, int expectedExitCode, Pattern pattern) {
        try {
            if (pr.exitCode() == expectedExitCode) {
                try (FileChannel channel = new FileInputStream(file).getChannel()) {
                    MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, (int) channel.size());
                    CharBuffer cs = Charset.defaultCharset().newDecoder().decode(byteBuffer);
                    Matcher m = pattern.matcher(cs);
                    if (m.find()) {
                        log.debug("Found output indicating success: {}", m.group());
                        return true;
                    }
                }
            }
            return false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ProcessHelper.ProcessResult exec(List<String> kafkaConfigsOptions) {
        String cp = System.getProperty("java.class.path");
        File home = new File(System.getProperty("java.home"));
        List<String> arguments = new ArrayList(asList(
                new File(home, "bin/java").getAbsolutePath(),
                "-cp", cp,
                ConfigCommand.class.getName()));
        arguments.addAll(kafkaConfigsOptions);

        try {
            return ProcessHelper.executeSubprocess(arguments);
        } catch (IOException e) {
            throw new RuntimeException("Error starting subprocess " + arguments, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        throw new RuntimeException("Error starting subprocess " + arguments);
    }

    private static String getFile(File out) {
        try {
            return new String(Files.readAllBytes(out.toPath()), Charset.defaultCharset());
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    public static void main(String[] args) {
        ScramShaCredentials scramSha = new ScramShaCredentials("localhost:2181");
        scramSha.createOrUpdate("tom", "password", -1);
        scramSha.createOrUpdate("tom", "password", 4096);
        scramSha.delete("tom");
    }
}

