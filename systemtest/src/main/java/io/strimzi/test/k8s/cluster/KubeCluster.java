/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.cluster;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.openshift.client.OpenShiftConfig;
import io.strimzi.test.k8s.exceptions.NoClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Locale;

/**
 * Abstraction for a Kubernetes cluster, for example {@code minikube}.
 */
public interface KubeCluster {

    String ENV_VAR_TEST_CLUSTER = "TEST_CLUSTER";
    OpenShiftConfig CONFIG = OpenShiftConfig.wrap(Config.autoConfigure(System.getenv().getOrDefault("TEST_CLUSTER_CONTEXT", null)));

    /** Return true iff this kind of cluster installed on the local machine. */
    boolean isAvailable();

    /** Return true iff this kind of cluster is running on the local machine */
    boolean isClusterUp();

    /**
     * Returns the cluster named by the TEST_CLUSTER environment variable, if set, otherwise finds a cluster that's
     * both installed and running.
     * @return The cluster.
     * @throws NoClusterException If no running cluster was found.
     */
    static KubeCluster bootstrap() throws NoClusterException {
        Logger logger = LogManager.getLogger(KubeCluster.class);

        KubeCluster[] clusters = null;
        String clusterName = System.getenv(ENV_VAR_TEST_CLUSTER);
        if (clusterName != null) {
            clusters = switch (clusterName.toLowerCase(Locale.ENGLISH)) {
                case "oc" -> new KubeCluster[]{new OpenShift()};
                case "minikube" -> new KubeCluster[]{new Minikube()};
                case "kind" -> new KubeCluster[]{new Kind()};
                case "microshift" -> new KubeCluster[]{new Microshift()};
                case "kubernetes" -> new KubeCluster[]{new Kubernetes()};
                default ->
                        throw new IllegalArgumentException(ENV_VAR_TEST_CLUSTER + "=" + clusterName + " is not a supported cluster type");
            };
        }
        if (clusters == null) {
            clusters = new KubeCluster[]{new Minikube(), new Kind(), new Kubernetes(), new OpenShift(), new Microshift()};
        }
        KubeCluster cluster = null;
        for (KubeCluster kc : clusters) {
            if (kc.isAvailable()) {
                logger.debug("Cluster {} is installed", kc);
                if (kc.isClusterUp()) {
                    logger.debug("Cluster {} is running", kc);
                    cluster = kc;
                    break;
                } else {
                    logger.debug("Cluster {} is not running!", kc);
                }
            } else {
                logger.debug("Cluster {} is not installed!", kc);
            }
        }
        if (cluster == null) {
            throw new NoClusterException(
                    "Unable to find a cluster; tried " + Arrays.toString(clusters));
        }
        logger.info("Using cluster: {}", cluster);
        return cluster;
    }
}
