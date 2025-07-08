/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.test.k8s.cluster.Kind;
import io.strimzi.test.k8s.cluster.KubeCluster;
import io.strimzi.test.k8s.cluster.Microshift;
import io.strimzi.test.k8s.cluster.Minikube;
import io.strimzi.test.k8s.cluster.OpenShift;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A Junit resource which discovers the running cluster and provides an appropriate KubeClient for it,
 * for use with {@code @BeforeAll} (or {@code BeforeEach}).
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

    private static final Logger LOGGER = LogManager.getLogger(KubeClusterResource.class);

    private KubeCluster kubeCluster;
    private static KubeClusterResource kubeClusterResource;

    private String namespace;

    public static synchronized KubeClusterResource getInstance() {
        if (kubeClusterResource == null) {
            kubeClusterResource = new KubeClusterResource();
            LOGGER.info("Cluster default namespace is '{}'", kubeClusterResource.getNamespace());
        }
        return kubeClusterResource;
    }

    private KubeClusterResource() { }

    /**
     * Sets the namespace value for Kubernetes clients
     * @param futureNamespace Namespace which should be used in Kubernetes clients
     * @return Previous namespace which was used in Kubernetes clients
     */
    public String setNamespace(String futureNamespace) {
        String previousNamespace = namespace;
        LOGGER.info("Client use Namespace: {}", futureNamespace);
        namespace = futureNamespace;
        return previousNamespace;
    }

    /**
     * Gets namespace which is used in Kubernetes clients at the moment
     * @return Used namespace
     */
    public String getNamespace() {
        return namespace;
    }

    public KubeCluster cluster() {
        if (kubeCluster == null) {
            kubeCluster = KubeCluster.bootstrap();
        }
        return kubeCluster;
    }

    public boolean isOpenShift() {
        return kubeClusterResource.cluster() instanceof OpenShift;
    }

    /**
     * Method determining if the cluster we are running tests on are "kind of" OpenShift
     * That means either OpenShift or MicroShift
     * @return boolean determining if we are running tests on OpenShift-like cluster
     */
    public boolean isOpenShiftLikeCluster() {
        return isOpenShift() || isMicroShift();
    }

    public boolean isKind() {
        return kubeClusterResource.cluster() instanceof Kind;
    }

    public boolean isMicroShift() {
        return kubeClusterResource.cluster() instanceof Microshift;
    }

    public boolean isMinikube() {
        return kubeClusterResource.cluster() instanceof Minikube;
    }

    public boolean fipsEnabled() {
        if (isOpenShift()) {
            return KubeResourceManager.get().kubeClient().getClient().configMaps()
                .inNamespace("kube-system").withName("cluster-config-v1").get().getData().get("install-config").contains("fips: true");
        }
        return false;
    }
}
