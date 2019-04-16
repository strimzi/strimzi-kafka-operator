/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import io.strimzi.test.client.Kubernetes;

/**
 * A Junit resource which discovers the running cluster and provides an appropriate KubeClient for it,
 * for use with {@code @BeforeAll} (or {@code BeforeEach}.
 * For example:
 * <pre><code>
 *     public static KubeClusterResource testCluster = new KubeClusterResources();
 *
 *     &#64;BeforeEach
 *     void before() {
 *         testCluster.before();
 *     }
 * </code></pre>
 */
public class KubeClusterResource {

    private final boolean bootstrap;
    private KubeCluster cluster;
    private KubeClient cmdClient;
    private Kubernetes client;
    private HelmClient helmClient;

    private static KubeClusterResource instance;

    public static synchronized KubeClusterResource getInstance() {
        if (instance == null) {
            instance = new KubeClusterResource();
        }
        return instance;
    }

    private KubeClusterResource() {
        bootstrap = true;
    }

    public KubeClusterResource(KubeCluster cluster, KubeClient client) {
        bootstrap = false;
        this.cluster = cluster;
        this.cmdClient = client;
    }

    public KubeClusterResource(KubeCluster cluster, KubeClient client, HelmClient helmClient) {
        bootstrap = false;
        this.cluster = cluster;
        this.cmdClient = client;
        this.helmClient = helmClient;
    }

    /** Gets the namespace in use */
    public String defaultNamespace() {
        return cmdClient().defaultNamespace();
    }

    public KubeClient cmdClient() {
        if (bootstrap && cmdClient == null) {
            this.cmdClient = cluster().defaultCmdClient();
        }
        return cmdClient;
    }

    public Kubernetes client() {
        if (bootstrap && client == null) {
            this.client = cluster().defaultClient();
        }
        return client;
    }

    public HelmClient helmClient() {
        if (bootstrap && helmClient == null) {
            this.helmClient = HelmClient.findClient(cmdClient());
        }
        return helmClient;
    }

    public KubeCluster cluster() {
        if (bootstrap && cluster == null) {
            this.cluster = KubeCluster.bootstrap();
        }
        return cluster;
    }

    public void before() {
        if (bootstrap) {
            if (cluster == null) {
                this.cluster = KubeCluster.bootstrap();
            }
            if (cmdClient == null) {
                this.cmdClient = cluster.defaultCmdClient();
            }
            if (client == null) {
                this.client = cluster.defaultClient();
            }
        }
    }
}
