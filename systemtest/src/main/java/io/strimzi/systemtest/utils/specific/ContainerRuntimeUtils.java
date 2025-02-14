/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ContainerRuntimeUtils {
    private static final String DOCKER = "docker";
    private static final String PODMAN = "podman";
    private static final String RUNTIME = detectRuntime();

    private static String detectRuntime() {
        if (isCommandAvailable(PODMAN)) {
            return PODMAN;
        } else if (isCommandAvailable(DOCKER)) {
            return DOCKER;
        } else {
            throw new IllegalStateException("Neither Podman nor Docker is available on the system.");
        }
    }

    private static boolean isCommandAvailable(String command) {
        try {
            Process process = new ProcessBuilder("which", command).start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                return reader.readLine() != null;
            }
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Returns the container runtime detected on the system.
     *
     * @return A string indicating the container runtime, either "podman" or "docker".
     * @throws IllegalStateException if neither Podman nor Docker is available.
     */
    public static String getRuntime() {
        return RUNTIME;
    }
}
