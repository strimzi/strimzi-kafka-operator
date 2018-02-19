/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * A Junit resource for using with {@code @ClassRule} (or {@code Rule},
 * but since starting and stopping a cluster is really slow, it's better to pay that cost as infrequently as possible).
 * For example:
 * <pre><code>
 *     @ClassRule
 *     public static KubeClusterResource testCluster;
 * </code></pre>
 *
 * If the {@code CI} environment variable is set, we assume a cluster has already been setup by CI
 *
 * Otherwise:
 * <ol>
 * <li>we search for binaries oc, minikube, minishift on the $PATH, using the first one found</li>
 * <li>we start that cluster (e.g. {code oc cluster up})</li>
 * </ol>
 *
 * We then setup {@link Role @Role}s and {@link RoleBinding @RoleBinding}s.
 */
public class KubeClusterResource extends ExternalResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubeClusterResource.class);
    private final boolean shouldStartCluster = System.getenv("CI") == null;
    private final KubeCluster cluster;
    private final KubeClient client;
    private final Thread clusterHook;
    private boolean startedCluster = false;
    private Class<?> testClass;

    public KubeClusterResource() {

        KubeCluster cluster = null;
        for (KubeCluster kc : new KubeCluster[]{new OpenShift(), Minikube.minikube(), Minikube.minishift()}) {
            if (kc.isAvailable()) {
                LOGGER.debug("Cluster {} is installed", kc);
                if (shouldStartCluster) {
                    LOGGER.debug("Using cluster {}", kc);
                    cluster = kc;
                    break;
                } else {
                    if (kc.isClusterUp()) {
                        LOGGER.debug("Cluster {} is running", kc);
                        cluster = kc;
                        break;
                    } else {
                        LOGGER.debug("Cluster {} is not running", kc);
                    }
                }
            } else {
                LOGGER.debug("Cluster {} is not installed", kc);
            }
        }
        this.cluster = cluster;
        this.clusterHook = new Thread(() -> {
            after();
        });
        KubeClient client = null;
        for (KubeClient kc: Arrays.asList(cluster.defaultClient(), new Kubectl(), new Oc())) {
            if (kc.clientAvailable()) {
                client = kc;
                break;
            }
        }
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
        if (shouldStartCluster) {
            if (cluster.isClusterUp()) {
                throw new RuntimeException("Cluster " + cluster + " is already up");
            }
            LOGGER.info("Starting cluster {}", cluster);
            // It can happen that if the VM exits abnormally the cluster remains up, and further tests don't work because
            // it appears there are two brokers with id 1, so use a shutdown hook to kill the cluster.
            startedCluster = true;
            cluster.clusterUp();
        } else if (cluster == null) {
            throw new KubeClusterException(-1, "I'm running in CI mode, so I can't start a cluster, " +
                    "but no clusters are running");
        }

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

        if (startedCluster) {
            startedCluster = false;
            try {
                LOGGER.info("Executing oc cluster down");
                cluster.clusterDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Runtime.getRuntime().removeShutdownHook(clusterHook);
    }

    /** Gets the namespace in use */
    public String defaultNamespace() {
        return client.defaultNamespace();
    }
}
