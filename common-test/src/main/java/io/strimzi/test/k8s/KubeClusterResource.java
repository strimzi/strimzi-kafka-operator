/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A Junit resource for using with {@code @ClassRule} (or {@code Rule},
 * but since starting and stopping a cluster is really slow, it's better to pay that cost as infrequently as possible).
 * For example:
 * <pre><code>
 *     @ClassRule
 *     public static KubeClusterResource testCluster;
 * </code></pre>
 *
 * Otherwise:
 * <ol>
 * <li>we search for binaries oc, minikube, minishift on the $PATH, using the first one that's both on the $PATH and running</li>
 * </ol>
 *
 * We then setup {@link Role @Role}s and {@link RoleBinding @RoleBinding}s.
 */
public class KubeClusterResource extends ExternalResource {

    private final KubeCluster cluster;
    private final KubeClient client;
    private final Thread clusterHook;
    private Class<?> testClass;

    public KubeClusterResource() {

        this.cluster = KubeCluster.bootstrap();
        this.clusterHook = new Thread(() -> {
            cleanup(true);
        });
        this.client = KubeClient.findClient(cluster);
    }

    public KubeClusterResource(KubeCluster cluster, Thread clusterHook, KubeClient client) {
        this.cluster = cluster;
        this.clusterHook = clusterHook;
        this.client = client;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        testClass = description.getTestClass();
        return super.apply(base, description);
    }

    @Override
    protected void before() {
        Runtime.getRuntime().addShutdownHook(clusterHook);

        Role role = testClass.getAnnotation(Role.class);
        if (role != null) {
            client.createRole(role.name(), role.permissions());
        }
        RoleBinding binding = testClass.getAnnotation(RoleBinding.class);
        if (binding != null) {
            client.createRoleBinding(binding.name(), binding.role(), binding.users());
        }
    }

    @Override
    protected void after() {
        cleanup(false);
    }
    protected void cleanup(boolean isHook) {
        try {
            RoleBinding binding = testClass.getAnnotation(RoleBinding.class);
            if (binding != null) {
                client.deleteRoleBinding(binding.name());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            Role role = testClass.getAnnotation(Role.class);
            if (role != null) {
                client.deleteRole(role.name());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (!isHook) {
            Runtime.getRuntime().removeShutdownHook(clusterHook);
        }
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
