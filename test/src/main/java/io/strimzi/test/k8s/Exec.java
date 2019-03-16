/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.join;

/**
 * Class provide execution of external command
 */
public class Exec {
    private static final Logger LOGGER = LogManager.getLogger(Exec.class);
    private static final Pattern ERROR_PATTERN = Pattern.compile("Error from server \\(([a-zA-Z0-9]+)\\):");
    private static final Pattern INVALID_PATTERN = Pattern.compile("The ([a-zA-Z0-9]+) \"([a-z0-9.-]+)\" is invalid:");
    private static final Pattern PATH_SPLITTER = Pattern.compile(System.getProperty("path.separator"));
    private static final Object LOCK = new Object();

    public Process process;
    private String stdOut;
    private String stdErr;
    private StreamGobbler stdOutReader;
    private StreamGobbler stdErrReader;
    private Path logPath;
    private boolean appendLineSeparator;

    public Exec() {
        this.appendLineSeparator = true;
    }

    public Exec(Path logPath) {
        this.appendLineSeparator = true;
        this.logPath = logPath;
    }

    public Exec(boolean appendLineSeparator) {
        this.appendLineSeparator = appendLineSeparator;
    }

    /**
     * Getter for stdOutput
     *
     * @return string stdOut
     */
    public String getStdOut() {
        return stdOut;
    }

    /**
     * Getter for stdErrorOutput
     *
     * @return string stdErr
     */
    public String getStdErr() {
        return stdErr;
    }

    public boolean isRunning() {
        return process.isAlive();
    }

    public int getRetCode() {
        LOGGER.info("Process: {}", process);
        if (isRunning()) {
            return -1;
        } else {
            return process.exitValue();
        }
    }


    /**
     * Method executes external command
     *
     * @param command arguments for command
     * @return execution results
     */
    public static ExecResult exec(String... command) {
        return exec(Arrays.asList(command));
    }

    /**
     * Method executes external command
     *
     * @param command arguments for command
     * @return execution results
     */
    public static ExecResult exec(List<String> command) {
        return exec(null, command, 0, false);
    }

    /**
     * Method executes external command
     *
     * @param command arguments for command
     * @return execution results
     */
    public static ExecResult exec(String input, List<String> command) {
        return exec(input, command, 0, false);
    }

    /**
     * Method executes external command
     * @param command arguments for command
     * @param timeout timeout for execution
     * @param logToOutput log output or not
     * @return execution results
     */
    public static ExecResult exec(String input, List<String> command, int timeout, boolean logToOutput) {
        int ret = 1;
        ExecResult execResult;
        try {
            Exec executor = new Exec();
            ret = executor.execute(input, command, timeout);
            synchronized (LOCK) {
                if (logToOutput) {
                    LOGGER.info("Return code: {}", ret);
                    LOGGER.info("stdout: \n{}", executor.getStdOut());
                    LOGGER.info("stderr: \n{}", executor.getStdErr());
                }
            }

            execResult = new ExecResult(ret, executor.getStdOut(), executor.getStdErr());

            if (ret != 0) {
                String msg = "`" + join(" ", command) + "` got status code " + ret + " and stderr:\n------\n" + executor.stdErr + "\n------\nand stdout:\n------\n" + executor.stdOut + "\n------";

                Matcher matcher = ERROR_PATTERN.matcher(executor.getStdErr());
                KubeClusterException t = null;

                if (matcher.find()) {
                    switch (matcher.group(1)) {
                        case "NotFound":
                            t = new KubeClusterException.NotFound(execResult, msg);
                            break;
                        case "AlreadyExists":
                            t = new KubeClusterException.AlreadyExists(execResult, msg);
                            break;
                        default:
                            break;
                    }
                }
                matcher = INVALID_PATTERN.matcher(executor.getStdErr());
                if (matcher.find()) {
                    t = new KubeClusterException.InvalidResource(execResult, msg);
                }
                if (t == null) {
                    t = new KubeClusterException(execResult, msg);
                }
                throw t;
            }
            return new ExecResult(ret, executor.getStdOut(), executor.getStdErr());

        } catch (IOException | ExecutionException e) {
            throw new KubeClusterException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KubeClusterException(e);
        }
    }

    /**
     * Method executes external command
     *
     * @param commands arguments for command
     * @param timeout  timeout in ms for kill
     * @return returns ecode of execution
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public int execute(String input, List<String> commands, int timeout) throws IOException, InterruptedException, ExecutionException {
        LOGGER.trace("Running command - " + join(" ", commands.toArray(new String[0])));
        ProcessBuilder builder = new ProcessBuilder();
        builder.command(commands);
        builder.directory(new File(System.getProperty("user.dir")));
        process = builder.start();
        OutputStream outputStream = process.getOutputStream();
        if (input != null) {
            LOGGER.trace("With stdin {}", input);
            outputStream.write(input.getBytes(Charset.defaultCharset()));
        }
        // Close subprocess' stdin
        outputStream.close();

        Future<String> output = readStdOutput();
        Future<String> error = readStdError();

        int retCode = 1;
        if (timeout > 0) {
            if (process.waitFor(timeout, TimeUnit.MILLISECONDS)) {
                retCode = process.exitValue();
            } else {
                process.destroyForcibly();
            }
        } else {
            retCode = process.waitFor();
        }

        try {
            stdOut = output.get(500, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            output.cancel(true);
            stdOut = stdOutReader.getData();
        }

        try {
            stdErr = error.get(500, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            error.cancel(true);
            stdErr = stdErrReader.getData();
        }
        storeOutputsToFile();

        return retCode;
    }

    /**
     * Method kills process
     */
    public void stop() {
        process.destroyForcibly();
        stdOut = stdOutReader.getData();
        stdErr = stdErrReader.getData();
    }

    /**
     * Get standard output of execution
     *
     * @return future string output
     */
    private Future<String> readStdOutput() {
        stdOutReader = new StreamGobbler(process.getInputStream());
        return stdOutReader.read();
    }

    /**
     * Get standard error output of execution
     *
     * @return future string error output
     */
    private Future<String> readStdError() {
        stdErrReader = new StreamGobbler(process.getErrorStream());
        return stdErrReader.read();
    }

    /**
     * Get stdOut and stdErr and store it into files
     */
    private void storeOutputsToFile() {
        if (logPath != null) {
            try {
                Files.createDirectories(logPath);
                Files.write(Paths.get(logPath.toString(), "stdOutput.log"), stdOut.getBytes());
                Files.write(Paths.get(logPath.toString(), "stdError.log"), stdErr.getBytes());
            } catch (Exception ex) {
                LOGGER.warn("Cannot save output of execution: " + ex.getMessage());
            }
        }
    }

    /**
     * Check if command is executable
     * @param cmd command
     * @return true.false
     */
    static boolean isExecutableOnPath(String cmd) {
        for (String dir : PATH_SPLITTER.split(System.getenv("PATH"))) {
            if (new File(dir, cmd).canExecute()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Class represent async reader
     */
    class StreamGobbler {
        private InputStream is;
        private StringBuilder data = new StringBuilder();

        /**
         * Constructor of StreamGobbler
         *
         * @param is input stream for reading
         */
        StreamGobbler(InputStream is) {
            this.is = is;
        }

        /**
         * Return data from stream sync
         *
         * @return string of data
         */
        public String getData() {
            return data.toString();
        }

        /**
         * read method
         *
         * @return return future string of output
         */
        public Future<String> read() {
            return CompletableFuture.supplyAsync(() -> {
                Scanner scanner = new Scanner(is);
                try {
                    while (scanner.hasNextLine()) {
                        data.append(scanner.nextLine());
                        if (appendLineSeparator) {
                            data.append(System.getProperty("line.separator"));
                        }
                    }
                    scanner.close();
                    return data.toString();
                } catch (Exception e) {
                    throw new CompletionException(e);
                } finally {
                    scanner.close();
                }
            }, runnable -> new Thread(runnable).start());
        }
    }
}