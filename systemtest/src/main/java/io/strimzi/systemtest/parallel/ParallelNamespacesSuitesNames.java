/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.parallel;

import io.strimzi.systemtest.Constants;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class responsible for all parallel namespaces names @see ParallelSuite.class. Moreover, it provides an auxiliary
 * method for getting all namespaces to watch and which are needed to bind. This ensures that namespaces can be run in
 * parallel when RBAC is set to NAMESPACE.
 */
public class ParallelNamespacesSuitesNames {

    public static final String BRIDGE_KAFKA_CORS_NAMESPACE = "bridge-kafka-cors-namespace";
    public static final String BRIDGE_KAFKA_EXTERNAL_LISTENER_NAMESPACE = "bridge-kafka-external-listener-namespace";
    public static final String BRIDGE_SCRAM_SHA_NAMESPACE = "bridge-scram-sha-namespace";
    public static final String BRIDGE_HTTP_TLS_NAMESPACE = "http-bridge-tls-namespace";
    public static final String METRICS_SECOND_NAMESPACE = "second-metrics-cluster-test";

    private static final List<String> PARALLEL_NAMESPACE_SUITE_NAMES = Arrays.asList(
        // default namespace for cluster operator
        Constants.INFRA_NAMESPACE,
        BRIDGE_KAFKA_CORS_NAMESPACE,
        BRIDGE_KAFKA_EXTERNAL_LISTENER_NAMESPACE,
        BRIDGE_SCRAM_SHA_NAMESPACE,
        BRIDGE_HTTP_TLS_NAMESPACE,
        METRICS_SECOND_NAMESPACE
    );

    public static String getRbacNamespacesToWatch() {
        return PARALLEL_NAMESPACE_SUITE_NAMES.stream()
            .map(parallelSuiteName -> "," + parallelSuiteName)
            .collect(Collectors.joining())
            .substring(1);
    }

    public static List<String> getBindingNamespaces() {
        return PARALLEL_NAMESPACE_SUITE_NAMES;
    }
}
