/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstraction for a Kubernetes cluster, for example {@code oc cluster up} or {@code minikube}.
 */
public interface KubeCluster {

    /** Return true iff this kind of cluster installed on the local machine. */
    boolean isAvailable();

    /** Return true iff this kind of cluster is running on the local machine */
    boolean isClusterUp();

    /** Attempt to start a cluster */
    void clusterUp();

    /** Attempt to stop a cluster */
    void clusterDown();

    /** Return a default client for this kind of cluster. */
    KubeClient defaultClient();

    static KubeCluster bootstrap() {
        Logger logger = LoggerFactory.getLogger(KubeCluster.class);
        final boolean shouldStartCluster = System.getenv("CI") == null;
        KubeCluster cluster = null;
        for (KubeCluster kc : new KubeCluster[]{new OpenShift(), Minikube.minikube(), Minikube.minishift()}) {
            if (kc.isAvailable()) {
                logger.debug("Cluster {} is installed", kc);
                if (shouldStartCluster) {
                    logger.debug("Using cluster {}", kc);
                    cluster = kc;
                    break;
                } else {
                    if (kc.isClusterUp()) {
                        logger.debug("Cluster {} is running", kc);
                        cluster = kc;
                        break;
                    } else {
                        logger.debug("Cluster {} is not running", kc);
                    }
                }
            } else {
                logger.debug("Cluster {} is not installed", kc);
            }
        }
        if (cluster == null) {
            throw new RuntimeException("Unable to find a cluster");
        }
        return cluster;
    }
}
