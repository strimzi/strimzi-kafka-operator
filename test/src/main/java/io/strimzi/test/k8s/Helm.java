/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import io.strimzi.test.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

public class Helm implements HelmClient {
    private static final Logger LOGGER = LogManager.getLogger(Helm.class);

    private static final String HELM_CMD = "helm";
    // TODO: configurable?
    private static final String INSTALL_TIMEOUT_SECONDS = "60";

    private KubeClient<?> kubeClient;
    private boolean initialized;

    public Helm(KubeClient<?> kubeClient) {
        this.kubeClient = kubeClient;
        this.initialized = false;
    }

    @Override
    public HelmClient init() {
        if (!initialized) {
            Exec.exec(wait(command("init", "--service-account", "tiller")));
            initialized = true;
        }
        return this;
    }

    @Override
    public HelmClient install(Path chart, String releaseName, Map<String, String> valuesMap) {
        String values = Stream.of(valuesMap).flatMap(m -> m.entrySet().stream())
                .map(entry -> String.format("%s=%s", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(","));
        Exec.exec(wait(namespace(command("install",
                "--name", releaseName,
                "--set-string", values,
                "--timeout", INSTALL_TIMEOUT_SECONDS,
                chart.toString()))));
        return this;
    }

    @Override
    public HelmClient delete(String releaseName) {
        // wait() not required, `helm delete` blocks by default
        Exec.exec(command("delete", releaseName, "--purge"));
        return this;
    }

    @Override
    public boolean clientAvailable() {
        return Exec.isExecutableOnPath(HELM_CMD);
    }

    private List<String> command(String... rest) {
        List<String> result = new ArrayList<>();
        result.add(HELM_CMD);
        result.addAll(asList(rest));
        return result;
    }

    private List<String> namespace(List<String> args) {
        args.add("--namespace");
        args.add(this.kubeClient.namespace());
        return args;
    }

    private List<String> wait(List<String> args) {
        args.add("--wait");
        return args;
    }
}
