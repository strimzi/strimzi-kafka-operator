/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.APIGroup;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.strimzi.operator.common.Util;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.platform.PlatformFeatures;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.util.Map;

/**
 * Provides info about certain features availability and Kubernetes version in the Kubernetes cluster
 */
public class PlatformFeaturesAvailability implements PlatformFeatures {
    private static final Logger LOGGER = LogManager.getLogger(PlatformFeaturesAvailability.class.getName());

    private boolean routes = false;
    private boolean builds = false;
    private boolean images = false;
    private KubernetesVersion kubernetesVersion;

    /**
     * Creates a PlatformFeaturesAvailability instance
     *
     * @param vertx     Vert.x instance
     * @param client    Kubernetes client
     *
     * @return  Instance of PlatformFeaturesAvailability
     */
    public static Future<PlatformFeaturesAvailability> create(Vertx vertx, KubernetesClient client) {
        Promise<PlatformFeaturesAvailability> pfaPromise = Promise.promise();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability();

        Future<VersionInfo> futureVersion = getVersionInfo(vertx, client);

        futureVersion.compose(versionInfo -> {
            String major = versionInfo.getMajor().isEmpty() ? Integer.toString(KubernetesVersion.MINIMAL_SUPPORTED_MAJOR) : versionInfo.getMajor();
            String minor = versionInfo.getMinor().isEmpty() ? Integer.toString(KubernetesVersion.MINIMAL_SUPPORTED_MINOR) : versionInfo.getMinor();
            pfa.setKubernetesVersion(new KubernetesVersion(Integer.parseInt(major.split("\\D")[0]), Integer.parseInt(minor.split("\\D")[0])));

            return checkApiAvailability(vertx, client, "route.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setRoutes(supported);
            return checkApiAvailability(vertx, client, "build.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setBuilds(supported);
            return checkApiAvailability(vertx, client, "image.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setImages(supported);
            return Future.succeededFuture(pfa);
        }).onComplete(pfaPromise);

        return pfaPromise.future();
    }

    /**
     * Gets the Kubernetes VersionInfo. It either used from the /version endpoint or from the STRIMZI_KUBERNETES_VERSION
     * environment variable. If defined, the environment variable will take the precedence. Otherwise, the API server
     * endpoint will be used.
     *
     * And example of the STRIMZI_KUBERNETES_VERSION environment variable in Cluster Operator deployment:
     * <pre><code>
     *       env:
     *         - name: STRIMZI_KUBERNETES_VERSION
     *           value: |
     *                 major=1
     *                 minor=16
     *                 gitVersion=v1.16.2
     *                 gitCommit=c97fe5036ef3df2967d086711e6c0c405941e14b
     *                 gitTreeState=clean
     *                 buildDate=2019-10-15T19:09:08Z
     *                 goVersion=go1.12.10
     *                 compiler=gc
     *                 platform=linux/amd64
     * </code></pre>
     *
     * @param vertx Instance of Vert.x
     * @param client    Fabric8 Kubernetes client
     * @return  Future with the VersionInfo object describing the Kubernetes version
     */
    private static Future<VersionInfo> getVersionInfo(Vertx vertx, KubernetesClient client) {
        Future<VersionInfo> futureVersion;

        String kubernetesVersion = System.getenv("STRIMZI_KUBERNETES_VERSION");

        if (kubernetesVersion != null) {
            try {
                futureVersion = Future.succeededFuture(parseVersionInfo(kubernetesVersion));
            } catch (ParseException e) {
                throw new RuntimeException("Failed to parse the Kubernetes version information provided through STRIMZI_KUBERNETES_VERSION environment variable", e);
            }
        } else {
            futureVersion = getVersionInfoFromKubernetes(vertx, client);
        }

        return futureVersion;
    }

    static VersionInfo parseVersionInfo(String str) throws ParseException {
        Map<String, String> map = Util.parseMap(str);
        VersionInfo.Builder vib = new VersionInfo.Builder();

        for (Map.Entry<String, String> entry: map.entrySet()) {
            switch (entry.getKey()) {
                case "major" -> vib.withMajor(map.get(entry.getKey()));
                case "minor" -> vib.withMinor(map.get(entry.getKey()));
                case "gitVersion" -> vib.withGitVersion(map.get(entry.getKey()));
                case "gitCommit" -> vib.withGitCommit(map.get(entry.getKey()));
                case "gitTreeState" -> vib.withGitTreeState(map.get(entry.getKey()));
                case "buildDate" -> vib.withBuildDate(map.get(entry.getKey()));
                case "goVersion" -> vib.withGoVersion(map.get(entry.getKey()));
                case "compiler" -> vib.withCompiler(map.get(entry.getKey()));
                case "platform" -> vib.withPlatform(map.get(entry.getKey()));
                default -> LOGGER.warn("Unknown key {} found", entry.getKey());
            }
        }

        return vib.build();
    }

    private static Future<VersionInfo> getVersionInfoFromKubernetes(Vertx vertx, KubernetesClient client)   {
        return vertx.executeBlocking(() -> {
            try {
                return client.getKubernetesVersion();
            } catch (Exception e) {
                LOGGER.error("Detection of Kubernetes version failed.", e);
                throw e;
            }
        });
    }

    private static Future<Boolean> checkApiAvailability(Vertx vertx, KubernetesClient client, String group, String version)   {
        return vertx.executeBlocking(() -> {
            try {
                APIGroup apiGroup = client.getApiGroup(group);
                boolean supported;

                if (apiGroup != null)   {
                    supported = apiGroup.getVersions().stream().anyMatch(v -> version.equals(v.getVersion()));
                } else {
                    supported = false;
                }

                LOGGER.warn("API Group {} is {}supported", group, supported ? "" : "not ");
                return supported;
            } catch (Exception e) {
                LOGGER.error("Detection of API availability failed.", e);
                throw e;
            }
        });
    }

    private PlatformFeaturesAvailability() {}

    /**
     * This constructor is used in tests. It sets all OpenShift APIs to true or false depending on the isOpenShift parameter
     *
     * @param isOpenShift           Set all OpenShift APIs to true
     * @param kubernetesVersion     Set the Kubernetes version
     */
    public PlatformFeaturesAvailability(boolean isOpenShift, KubernetesVersion kubernetesVersion) {
        this.kubernetesVersion = kubernetesVersion;
        this.routes = isOpenShift;
        this.images = isOpenShift;
        this.builds = isOpenShift;
    }

    @Override
    public boolean isOpenshift() {
        return this.hasRoutes();
    }

    @Override
    public KubernetesVersion getKubernetesVersion() {
        return this.kubernetesVersion;
    }

    private void setKubernetesVersion(KubernetesVersion kubernetesVersion) {
        this.kubernetesVersion = kubernetesVersion;
    }

    /**
     * @return  True if OpenShift Routes are supported on this cluster
     */
    public boolean hasRoutes() {
        return routes;
    }

    private void setRoutes(boolean routes) {
        this.routes = routes;
    }

    /**
     * @return  True if OpenShift Builds are supported on this cluster
     */
    public boolean hasBuilds() {
        return builds;
    }

    private void setBuilds(boolean builds) {
        this.builds = builds;
    }

    /**
     * @return  True if OpenShift ImageStreams are supported on this cluster
     */
    public boolean hasImages() {
        return images;
    }

    private void setImages(boolean images) {
        this.images = images;
    }

    /**
     * @return  True if OpenShift S2I (Builds and Images) are supported on this cluster
     */
    public boolean supportsS2I() {
        return hasBuilds() && hasImages();
    }

    @Override
    public String toString() {
        return "PlatformFeaturesAvailability(" +
                "KubernetesVersion=" + kubernetesVersion +
                ",OpenShiftRoutes=" + routes +
                ",OpenShiftBuilds=" + builds +
                ",OpenShiftImageStreams=" + images +
                ")";
    }
}
