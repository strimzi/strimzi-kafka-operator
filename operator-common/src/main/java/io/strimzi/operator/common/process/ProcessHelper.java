/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ProcessHelper {

    private static final Logger LOGGER = LogManager.getLogger(ProcessHelper.class);

    public static ProcessResult executeSubprocess(List<String> verifyArgs) throws IOException, InterruptedException {
        if (verifyArgs.isEmpty() || !new File(verifyArgs.get(0)).canExecute()) {
            throw new RuntimeException("Command " + verifyArgs + " lacks an executable arg[0]");
        }

        ProcessBuilder pb = new ProcessBuilder(verifyArgs);
        // If we redirect stderr to stdout we could break clients which parse the output because the
        // characters will be jumbled.
        // Reading two pipes without deadlocking on the blocking is difficult, so let's just write stderr to a file.
        File stdout = createTmpFile(".out");
        File stderr = createTmpFile(".err");
        pb.redirectError(stderr);
        pb.redirectOutput(stdout);
        Process p = pb.start();
        LOGGER.info("Started process {} with command line {}", p, verifyArgs);
        p.getOutputStream().close();
        int exitCode = p.waitFor();
        // TODO timeout on wait
        LOGGER.info("Process {}: exited with status {}", p, exitCode);
        return new ProcessResult(p, stdout, stderr);
    }

    public static File createTmpFile(String suffix) throws IOException {
        File tmpFile = File.createTempFile(ProcessHelper.class.getName(), suffix);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Created temporary file {}", tmpFile);
        }
        tmpFile.deleteOnExit();
        return tmpFile;
    }

    public static void delete(File file) {
        if (!file.delete()) {
            LOGGER.warn("Unable to delete temporary file {}", file);
        }
    }

    public static class ProcessResult implements AutoCloseable {
        private final File stdout;
        private final File stderr;
        private final Process process;

        ProcessResult(Process process, File stdout, File stderr) {
            this.process = process;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        public int exitCode() {
            return this.process.exitValue();
        }

        public File standardOutput() {
            return stdout;
        }

        public File standardError() {
            return stderr;
        }

        @Override
        public void close() {
            delete(stdout);
            delete(stderr);
        }
    }
}
