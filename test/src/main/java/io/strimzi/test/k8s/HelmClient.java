/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.cmdClient.KubeCmdClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

public class HelmClient {
    private static final Logger LOGGER = LogManager.getLogger(HelmClient.class);

    private static final String HELM_CMD = "helm";
    private static final String HELM_3_CMD = "helm3";
    private static final String INSTALL_TIMEOUT_SECONDS = "120s";

    private boolean initialized;
    private String namespace;

    private static String helmCommand = HELM_CMD;

    public HelmClient(String namespace) {
        this.namespace = namespace;
    }

    public HelmClient namespace(String namespace) {
        return new HelmClient(namespace);
    }

    /** Install a chart given its local path, release name, and values to override */
    public HelmClient install(Path chart, String releaseName, Map<String, String> valuesMap) {
        LOGGER.info("Installing helm-chart {}", releaseName);
        String values = Stream.of(valuesMap).flatMap(m -> m.entrySet().stream())
                .map(entry -> String.format("%s=%s", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(","));
        Exec.exec(wait(namespace(command("install",
                releaseName,
                "--set-string", values,
                "--timeout", INSTALL_TIMEOUT_SECONDS,
                chart.toString()))));
        return this;
    }

    /** Delete a chart given its release name */
    public HelmClient delete(String releaseName) {
        LOGGER.info("Deleting helm-chart {}", releaseName);
        Exec.exec(null, namespace(command("delete", releaseName)), 0, false, false);
        return this;
    }

    public static boolean clientAvailable() {
        if (Exec.isExecutableOnPath(HELM_CMD)) {
            return true;
        } else if (Exec.isExecutableOnPath(HELM_3_CMD)) {
            helmCommand = HELM_3_CMD;
            return true;
        }
        return false;
    }

    private List<String> command(String... rest) {
        List<String> result = new ArrayList<>();
        result.add(helmCommand);
        result.addAll(asList(rest));
        return result;
    }

    /** Sets namespace for client */
    private List<String> namespace(List<String> args) {
        args.add("--namespace");
        args.add(namespace);
        return args;
    }

    private List<String> wait(List<String> args) {
        args.add("--wait");
        return args;
    }

    static HelmClient findClient(KubeCmdClient<?> kubeClient) {
        HelmClient client = new HelmClient(kubeClient.namespace());
        if (!client.clientAvailable()) {
            throw new RuntimeException("No helm client found on $PATH. $PATH=" + System.getenv("PATH"));
        }
        return client;
    }
}
