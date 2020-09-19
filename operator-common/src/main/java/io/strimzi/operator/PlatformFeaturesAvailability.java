/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.strimzi.operator.common.Util;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.util.Map;

/**
 * Gives a info about certain features availability regarding to kubernetes version
 */
public class PlatformFeaturesAvailability {
    private static final Logger log = LogManager.getLogger(PlatformFeaturesAvailability.class.getName());

    private boolean routes = false;
    private boolean builds = false;
    private boolean images = false;
    private boolean apps = false;
    private KubernetesVersion kubernetesVersion;

    public static Future<PlatformFeaturesAvailability> create(Vertx vertx, KubernetesClient client) {
        Promise<PlatformFeaturesAvailability> pfaPromise = Promise.promise();
        OkHttpClient httpClient = getOkHttpClient(client);

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability();

        Future<VersionInfo> futureVersion = getVersionInfo(vertx, client);

        futureVersion.compose(versionInfo -> {
            String major = versionInfo.getMajor().equals("") ? Integer.toString(KubernetesVersion.MINIMAL_SUPPORTED_MAJOR) : versionInfo.getMajor();
            String minor = versionInfo.getMinor().equals("") ? Integer.toString(KubernetesVersion.MINIMAL_SUPPORTED_MINOR) : versionInfo.getMinor();
            pfa.setKubernetesVersion(new KubernetesVersion(Integer.parseInt(major.split("\\D")[0]), Integer.parseInt(minor.split("\\D")[0])));

            return checkApiAvailability(vertx, httpClient, client.getMasterUrl().toString(), "route.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setRoutes(supported);
            return checkApiAvailability(vertx, httpClient, client.getMasterUrl().toString(), "build.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setBuilds(supported);
            return checkApiAvailability(vertx, httpClient, client.getMasterUrl().toString(), "apps.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setApps(supported);
            return checkApiAvailability(vertx, httpClient, client.getMasterUrl().toString(), "image.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setImages(supported);
            return Future.succeededFuture(pfa);
        })
        .onComplete(pfaPromise);

        return pfaPromise.future();
    }

    private static OkHttpClient getOkHttpClient(KubernetesClient client)   {
        if (client.isAdaptable(OkHttpClient.class)) {
            return client.adapt(OkHttpClient.class);
        } else {
            log.error("Cannot adapt KubernetesClient to OkHttpClient");
            throw new RuntimeException("Cannot adapt KubernetesClient to OkHttpClient");
        }
    }

    /**
     * Gets the Kubernetes VersionInfo. It either used from the /version endpoint or from the STRIMZI_KUBERNETES_VERSION
     * environment variable. If defined, the environment variable will take the precedence. Otherwise the API server
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
            if (entry.getKey().equals("major")) {
                vib.withMajor(map.get(entry.getKey()));
            } else if (entry.getKey().equals("minor")) {
                vib.withMinor(map.get(entry.getKey()));
            } else if (entry.getKey().equals("gitVersion")) {
                vib.withGitVersion(map.get(entry.getKey()));
            } else if (entry.getKey().equals("gitCommit")) {
                vib.withGitCommit(map.get(entry.getKey()));
            } else if (entry.getKey().equals("gitTreeState")) {
                vib.withGitTreeState(map.get(entry.getKey()));
            } else if (entry.getKey().equals("buildDate")) {
                vib.withBuildDate(map.get(entry.getKey()));
            } else if (entry.getKey().equals("goVersion")) {
                vib.withGoVersion(map.get(entry.getKey()));
            } else if (entry.getKey().equals("compiler")) {
                vib.withCompiler(map.get(entry.getKey()));
            } else if (entry.getKey().equals("platform")) {
                vib.withPlatform(map.get(entry.getKey()));
            }
        }
        return vib.build();
    }

    private static Future<VersionInfo> getVersionInfoFromKubernetes(Vertx vertx, KubernetesClient client)   {
        Promise<VersionInfo> promise = Promise.promise();

        vertx.executeBlocking(request -> {
            try {
                request.complete(client.getVersion());
            } catch (Exception e) {
                log.error("Detection of Kubernetes version failed.", e);
                request.fail(e);
            }
        }, promise);

        return promise.future();
    }

    private static Future<Boolean> checkApiAvailability(Vertx vertx, OkHttpClient httpClient, String masterUrl, String api, String version)   {
        Promise<Boolean> promise = Promise.promise();

        vertx.executeBlocking(request -> {
            try {
                Boolean isSupported;

                Response resp = httpClient.newCall(new Request.Builder().get().url(masterUrl + "apis/" + api + "/" + version).build()).execute();
                if (resp.code() >= 200 && resp.code() < 300) {
                    log.debug("{} returned {}. This API is supported.", resp.request().url(), resp.code());
                    isSupported = true;
                } else {
                    log.debug("{} returned {}. This API is not supported.", resp.request().url(), resp.code());
                    isSupported = false;
                }

                resp.close();
                request.complete(isSupported);
            } catch (Exception e) {
                log.error("Detection of {}/{} API failed. This API will be disabled.", api, version, e);
                request.complete(false);
            }
        }, promise);

        return promise.future();
    }

    private PlatformFeaturesAvailability() {}

    /**
     * This constructor is used in tests. It sets all OpenShift APIs to true or false depending on the isOpenShift paremeter
     *
     * @param isOpenShift           Set all OpenShift APIs to true
     * @param kubernetesVersion     Set the Kubernetes version
     */
    /* test */public PlatformFeaturesAvailability(boolean isOpenShift, KubernetesVersion kubernetesVersion) {
        this.kubernetesVersion = kubernetesVersion;
        this.routes = isOpenShift;
        this.images = isOpenShift;
        this.builds = isOpenShift;
        this.apps = isOpenShift;
    }

    public boolean isOpenshift() {
        return this.hasRoutes();
    }

    public boolean isNamespaceAndPodSelectorNetworkPolicySupported() {
        if (isOpenshift()) {
            return this.kubernetesVersion.compareTo(KubernetesVersion.V1_12) >= 0;
        } else {
            return this.kubernetesVersion.compareTo(KubernetesVersion.V1_11) >= 0;
        }
    }

    public KubernetesVersion getKubernetesVersion() {
        return this.kubernetesVersion;
    }

    private void setKubernetesVersion(KubernetesVersion kubernetesVersion) {
        this.kubernetesVersion = kubernetesVersion;
    }

    public boolean hasRoutes() {
        return routes;
    }

    private void setRoutes(boolean routes) {
        this.routes = routes;
    }

    public boolean hasBuilds() {
        return builds;
    }

    private void setBuilds(boolean builds) {
        this.builds = builds;
    }

    public boolean hasImages() {
        return images;
    }

    private void setImages(boolean images) {
        this.images = images;
    }

    public boolean hasApps() {
        return apps;
    }

    private void setApps(boolean apps) {
        this.apps = apps;
    }

    public boolean supportsS2I() {
        return hasBuilds() && hasApps() && hasImages();
    }

    @Override
    public String toString() {
        return "ClusterOperatorConfig(" +
                "KubernetesVersion=" + kubernetesVersion +
                ",OpenShiftRoutes=" + routes +
                ",OpenShiftBuilds=" + builds +
                ",OpenShiftImageStreams=" + images +
                ",OpenShiftDeploymentConfigs=" + apps +
                ")";
    }
}
