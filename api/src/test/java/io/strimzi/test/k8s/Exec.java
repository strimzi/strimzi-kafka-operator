/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.join;
import static java.util.Arrays.asList;

class Exec {
    private static final Logger LOGGER = LogManager.getLogger(Exec.class);

    /**
     * Executes the given command in a subprocess.
     * @throws KubeClusterException if the process returned a non-zero status code, or if anything else went wrong.
     */
    static ProcessResult exec(String... cmd) throws KubeClusterException {
        return exec(Arrays.asList(cmd));
    }

    /**
     * Executes the given command in a subprocess.
     * @throws KubeClusterException if the process returned a non-zero status code, or if anything else went wrong.
     */
    static ProcessResult exec(List<String> cmd) throws KubeClusterException {
        return exec(null, cmd);
    }
    static ProcessResult exec(String input, List<String> cmd) throws KubeClusterException {
        File out = null;
        File err = null;
        try {
            out = File.createTempFile(Exec.class.getName(), Integer.toString(cmd.hashCode()));
            out.deleteOnExit();
            err = File.createTempFile(Exec.class.getName(), Integer.toString(cmd.hashCode()));
            err.deleteOnExit();

            LOGGER.trace("{}", join(" ", cmd));
            ProcessBuilder pb = new ProcessBuilder(cmd)
                    .redirectOutput(out)
                    .redirectError(err);
            Process p = pb.start();
            OutputStream outputStream = p.getOutputStream();
            if (input != null) {
                LOGGER.trace("With stdin {}", input);
                outputStream.write(input.getBytes(Charset.defaultCharset()));
            }
            // Close subprocess' stdin
            outputStream.close();

            int sc = p.waitFor();
            String stderr = new String(Files.readAllBytes(err.toPath()), Charset.defaultCharset());
            String stdout = new String(Files.readAllBytes(out.toPath()), Charset.defaultCharset());
            ProcessResult result = new ProcessResult(sc, stdout, stderr);
            if (sc != 0) {
                String msg = "`" + join(" ", cmd) + "` got status code " + sc + " and stderr:\n------\n" + stderr + "\n------\nand stdout:\n------\n" + stdout + "\n------";
                Pattern existenceRe = Pattern.compile("Error from server \\(([a-zA-Z0-9]+)\\):");
                Matcher matcher = existenceRe.matcher(stderr);
                KubeClusterException t = null;
                if (matcher.find()) {
                    switch (matcher.group(1)) {
                        case "NotFound":
                            t = new KubeClusterException.NotFound(result, msg);
                            break;
                        case "AlreadyExists":
                            t = new KubeClusterException.AlreadyExists(result, msg);
                            break;
                        default:
                            break;
                    }
                }
                Pattern invalidRe = Pattern.compile("The ([a-zA-Z0-9]+) \"([a-z0-9.-]+)\" is invalid:");
                matcher = invalidRe.matcher(stderr);
                if (matcher.find()) {
                    t = new KubeClusterException.InvalidResource(result, msg);
                }
                if (t == null) {
                    t = new KubeClusterException(result, msg);
                }
                throw t;
            }

            return result;
        } catch (IOException e) {
            throw new KubeClusterException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KubeClusterException(e);
        } finally {
            if (out != null) {
                if (!out.delete()) {
                    LOGGER.error("Failed to delete temporary file");
                }
            }
            if (err != null) {
                if (!err.delete()) {
                    LOGGER.error("Failed to delete temporary file");
                }
            }
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

    static List<String> filesAsStrings(File... files) {
        return filesAsStrings(asList(files));
    }

    static List<String> filesAsStrings(List<File> files) {
        ArrayList<String> result = new ArrayList<>();
        filesAsStrings(files, result);
        result.sort(null);
        return result;
    }

    static List<String> filesAsStrings(List<File> files, List<String> result) {
        for (File f : files) {
            if (f.isFile()) {
                result.add("-f");
                result.add(f.getAbsolutePath());
            } else if (f.isDirectory()) {
                File[] children = f.listFiles();
                if (children != null) {
                    filesAsStrings(asList(children), result);
                }
            }
        }
        return result;
    }

}
