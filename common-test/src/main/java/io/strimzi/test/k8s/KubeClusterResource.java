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

    private final KubeCluster cluster;
    private final KubeClient client;

    public KubeClusterResource() {
        this.cluster = KubeCluster.bootstrap();
        this.client = KubeClient.findClient(cluster);
    }

    public KubeClusterResource(KubeCluster cluster, KubeClient client) {
        this.cluster = cluster;
        this.client = client;
    }

    /** Gets the namespace in use */
    public String defaultNamespace() {
        return client().defaultNamespace();
    }

    public KubeClient client() {
        return client;
    }

    public KubeCluster cluster() {
        return cluster;
    }
}
