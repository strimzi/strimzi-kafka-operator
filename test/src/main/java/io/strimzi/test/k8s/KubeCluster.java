/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Locale;

/**
 * Abstraction for a Kubernetes cluster, for example {@code oc cluster up} or {@code minikube}.
 */
public interface KubeCluster {

    String ENV_VAR_TEST_CLUSTER = "TEST_CLUSTER";

    /** Return true iff this kind of cluster installed on the local machine. */
    boolean isAvailable();

    /** Return true iff this kind of cluster is running on the local machine */
    boolean isClusterUp();

    /** Attempt to start a cluster */
    void clusterUp();

    /** Attempt to stop a cluster */
    void clusterDown();

    /** Return a default CMD cmdClient for this kind of cluster. */
    KubeClient defaultCmdClient();

    Kubernetes defaultClient();

    /**
     * Returns the cluster named by the TEST_CLUSTER environment variable, if set, otherwise finds a cluster that's
     * both installed and running.
     * @return The cluster.
     * @throws RuntimeException If no running cluster was found.
     */
    static KubeCluster bootstrap() {
        Logger logger = LogManager.getLogger(KubeCluster.class);

        KubeCluster[] clusters = null;
        String clusterName = System.getenv(ENV_VAR_TEST_CLUSTER);
        if (clusterName != null) {
            switch (clusterName.toLowerCase(Locale.ENGLISH)) {
                case "oc":
                    clusters = new KubeCluster[]{new OpenShift()};
                    break;
                case "minikube":
                    clusters = new KubeCluster[]{new Minikube()};
                    break;
                case "minishift":
                    clusters = new KubeCluster[]{new Minishift()};
                    break;
                default:
                    throw new IllegalArgumentException(ENV_VAR_TEST_CLUSTER + "=" + clusterName + " is not a supported cluster type");
            }
        }
        if (clusters == null) {
            clusters = new KubeCluster[]{new OpenShift(), new Minikube(), new Minishift()};
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
                    logger.debug("Cluster {} is not running", kc);
                }
            } else {
                logger.debug("Cluster {} is not installed", kc);
            }
        }
        if (cluster == null) {
            throw new RuntimeException("Unable to find a cluster; tried " + Arrays.toString(clusters));
        }
        return cluster;
    }
}
