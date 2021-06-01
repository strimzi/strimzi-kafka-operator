/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.process;

import io.strimzi.operator.common.ReconciliationLogger;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ProcessHelper {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ProcessHelper.class);

    /**
     * Execute the command given in {@code args}.
     * Apply the given {@code sanitizer} function to the
     * {@code args} when logging, so that security-sensitive information is not logged.
     *
     * @param args The executable and its arguments.
     * @return The result of the subprocess
     * @throws IOException Reading/writing to the subprocess
     * @throws InterruptedException If interrupted while waiting for the subprocess to complete
     */
    public static ProcessResult executeSubprocess(List<String> args) throws IOException, InterruptedException {

        if (args.isEmpty() || !new File(args.get(0)).canExecute()) {
            throw new RuntimeException("Command " + args + " lacks an executable arg[0]");
        }

        ProcessBuilder pb = new ProcessBuilder(args);
        // If we redirect stderr to stdout we could break clients which parse the output because the
        // characters will be jumbled.
        // Reading two pipes without deadlocking on the blocking is difficult, so let's just write stderr to a file.
        File stdout = createTmpFile(".out");
        File stderr = createTmpFile(".err");
        pb.redirectError(stderr);
        pb.redirectOutput(stdout);
        Process p = pb.start();
        LOGGER.infoOp("Started process {} with command line {}", p, args);
        p.getOutputStream().close();
        int exitCode = p.waitFor();
        // TODO timeout on wait
        LOGGER.infoOp("Process {}: exited with status {}", p, exitCode);
        return new ProcessResult(p, stdout, stderr);
    }

    public static File createTmpFile(String suffix) throws IOException {
        File tmpFile = File.createTempFile(ProcessHelper.class.getName(), suffix);
        tmpFile.deleteOnExit();
        return tmpFile;
    }

    public static void delete(File file) {
        if (!file.delete()) {
            LOGGER.warnOp("Unable to delete temporary file {}", file);
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
