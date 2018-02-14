/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static java.lang.String.join;

class Exec {
    private static final Logger LOGGER = LoggerFactory.getLogger(Exec.class);

    /**
     * Executes the given command in a subprocess.
     * @throws KubeClusterException if the process returned a non-zero status code, or if anything else went wrong.
     */
    static void exec(String... cmd) throws KubeClusterException {
        exec(Arrays.asList(cmd));
    }

    /**
     * Executes the given command in a subprocess.
     * @throws KubeClusterException if the process returned a non-zero status code, or if anything else went wrong.
     */
    static void exec(List<String> cmd) throws KubeClusterException {
        execOutput(null, cmd);
    }

    /**
     * Executes the given command in a subprocess and returns the standard output generated.
     * @throws KubeClusterException if the process returned a non-zero status code, or if anything else went wrong.
     */
    static String execOutput(String... cmd) throws KubeClusterException {
        try {
            File tmp = File.createTempFile(Exec.class.getName(), Integer.toString(Arrays.hashCode(cmd)));
            try {
                tmp.deleteOnExit();
                return execOutput(tmp, Arrays.asList(cmd));
            } finally {
                tmp.delete();
            }
        } catch (IOException e) {
            throw new KubeClusterException(e);
        }
    }

    private static String execOutput(File out, List<String> cmd) throws KubeClusterException {
        try {
            LOGGER.info("{}", join(" ", cmd));
            ProcessBuilder pb = new ProcessBuilder(cmd);
            if (out == null) {
                pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            } else {
                pb.redirectOutput(out);
            }
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            Process p = pb.start();
            int sc = p.waitFor();
            if (sc != 0) {
                throw new KubeClusterException(sc, "`" + join(" ", cmd) + "` got status code " + sc);
            }
            return out == null ? null : new String(Files.readAllBytes(out.toPath()));
        } catch (IOException e) {
            throw new KubeClusterException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KubeClusterException(e);
        }
    }

    static boolean isExecutableOnPath(String cmd) {
        for (String dir: System.getenv("PATH").split(Pattern.quote(System.getProperty("path.separator")))) {
            if (new File(dir, cmd).canExecute()) {
                return true;
            }
        }
        return false;
    }

}
