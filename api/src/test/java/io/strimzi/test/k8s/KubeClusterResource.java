/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import org.junit.rules.ExternalResource;

/**
 * A Junit resource which discovers the running cluster and provides an appropriate KubeClient for it,
 * for use with {@code @ClassRule} (or {@code Rule}.
 * For example:
 * <pre><code>
 *     @ClassRule
 *     public static KubeClusterResource testCluster;
 * </code></pre>
 */
public class KubeClusterResource extends ExternalResource {

    private final boolean bootstrap;
    private KubeCluster cluster;
    private KubeClient client;

    public KubeClusterResource() {
        bootstrap = true;
    }

    public KubeClusterResource(KubeCluster cluster, KubeClient client) {
        bootstrap = false;
        this.cluster = cluster;
        this.client = client;
    }

    /** Gets the namespace in use */
    public String defaultNamespace() {
        return client().defaultNamespace();
    }

    public KubeClient client() {
        if (bootstrap && client == null) {
            this.client = KubeClient.findClient(cluster());
        }
        return client;
    }

    public KubeCluster cluster() {
        if (bootstrap && cluster == null) {
            this.cluster = KubeCluster.bootstrap();
        }
        return cluster;
    }

    @Override
    public void before() {
        if (bootstrap) {
            if (cluster == null) {
                this.cluster = KubeCluster.bootstrap();
            }
            if (client == null) {
                this.client = KubeClient.findClient(cluster);
            }
        }
    }
}
